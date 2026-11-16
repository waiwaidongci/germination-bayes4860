"""韧性运行服务。

设计要点：
- 数据库是排队与状态的唯一事实源；Redis列表仅作唤醒通知，通知丢失由定时扫描兜底，
  因此不依赖通知也能找到全部任务。
- 同一研究同情景（阈值/未来m/提交时输入指纹）在排队或计算中只保留一个运行（去重键），
  重提返回既有运行，不另建研究；进入终态后去重键置空，允许作为历史新建。
- 领取采用条件更新+行锁，保证并发只有一个worker成功；计算持租约并周期性续约，
  租约超时可被接管，迟到实例的条件更新失败，其结果绝不发布。
- 先验拟合与逐批预测分段落盘续算；完整结果原子写盘后再条件落库，文件已写而状态未落库
  也能由扫描回填；只有自洽完整结果可查看/下载。
"""
import csv, io, logging
from datetime import timedelta
from redis import Redis
from fastapi import HTTPException
from sqlalchemy import update, or_
from sqlalchemy.exc import IntegrityError
from ..models import BayesRun, SamplingNote
from ..config.settings import REDIS_URL, LEASE_TTL_SECONDS
from ..utils.serialize import record
from ..utils.clock import now, parse, lease_expired
from ..utils.fingerprint import fingerprint
from .study_service import get_study, create_snapshot
from .analysis_service import prepare_snapshot
from . import segment_service
from .audit_service import log

logger = logging.getLogger("germination.run")
queue = Redis.from_url(REDIS_URL, decode_responses=True)
JOB_KEY = "germination:jobs"
ACTIVE_STATUSES = ("queued", "running")

# —— 去重键与情景指纹 ——
def scenario_fingerprint(snapshot, threshold, future_n):
    lots, ordered, _, _ = prepare_snapshot(snapshot)
    return fingerprint({
        "study_id": snapshot["study"]["id"],
        "threshold": float(threshold),
        "future_n": int(future_n),
        "lots": [{"code": l["code"], "n": c["n"], "k": c["k"]} for l, c in zip(lots, ordered)],
    })

def make_dedup_key(snapshot, threshold, future_n):
    return "run:" + scenario_fingerprint(snapshot, threshold, future_n)

def list_runs(db):
    return [record(r) for r in db.query(BayesRun).order_by(BayesRun.id.desc())]

def get_run(db, rid):
    row = db.get(BayesRun, rid)
    if not row:
        raise HTTPException(404, "批次概率运行不存在")
    return row

def find_active_by_dedup(db, dedup_key):
    return db.query(BayesRun).filter(BayesRun.dedup_key == dedup_key, BayesRun.status.in_(ACTIVE_STATUSES)).first()

def create_run(db, data, user):
    study = get_study(db, data.study_id)
    snapshot = create_snapshot(db, study)
    try:
        prepare_snapshot(snapshot)
    except ValueError as e:
        raise HTTPException(422, str(e))
    dedup_key = make_dedup_key(snapshot, data.threshold, data.future_n)
    existing = find_active_by_dedup(db, dedup_key)
    if existing:
        # 排队/计算中同情景重提：复用同一研究，绝不另建
        existing.progress = {**(existing.progress or {}), "detail": "重复提交已合并到该运行", "updated_at": now()}
        db.commit()
        _notify(existing.id)
        logger.info("run %s reused for duplicate submission", existing.id)
        return {**record(existing), "reused": True}
    row = BayesRun(**data.model_dump(), input_snapshot=snapshot, owner=user.name,
                   status="queued", dedup_key=dedup_key, attempts=0, recoveries=0,
                   progress={"stage": "queued", "total": 0, "completed": 0, "detail": "已排队等待计算", "updated_at": now()})
    db.add(row)
    try:
        # 保存点：并发同情景提交时由部分唯一索引兜底（Postgres），失败则回退到复用逻辑
        with db.begin_nested():
            db.flush()
            log(db, user.email, "bayes.enqueue", row.id)
        db.commit()
    except IntegrityError:
        db.rollback()
        other = find_active_by_dedup(db, dedup_key)
        if other:
            _notify(other.id)
            return {**record(other), "reused": True}
        raise HTTPException(409, "同情景研究已在排队或计算中")
    _notify(row.id)
    return {**record(row), "reused": False}

# —— Redis通知（尽力而为；丢失由扫描兜底）——
def _notify(run_id):
    try:
        # 同一运行在通知列表里只保留一份，避免重复唤醒
        queue.lrem(JOB_KEY, 0, str(run_id))
        queue.rpush(JOB_KEY, run_id)
    except Exception:
        logger.warning("redis notify failed for run %s; sweeper will recover", run_id)

def wait_notification(timeout=2):
    try:
        job = queue.blpop(JOB_KEY, timeout=timeout)
        return int(job[1]) if job else None
    except Exception:
        logger.warning("redis blpop unavailable; relying on database sweep")
        return None

# —— 领取：并发安全，只有一个worker成功 ——
def claim_next_run(db, worker_id):
    """领取一个可运行任务：排队任务优先，其次超时的运行中任务。

    用条件UPDATE原子领取：数据库行锁/写锁串行化并发，未命中条件的worker得到rowcount=0。
    """
    until = (parse(now()) + timedelta(seconds=LEASE_TTL_SECONDS)).isoformat()
    queued_ids = [r for (r,) in db.query(BayesRun.id).filter(BayesRun.status == "queued").order_by(BayesRun.id).all()]
    candidates = [(rid, False) for rid in queued_ids]
    for (rid, lease) in db.query(BayesRun.id, BayesRun.lease_until).filter(BayesRun.status == "running").all():
        if lease_expired(lease):
            candidates.append((rid, True))
    for rid, recovered in candidates:
        if _atomic_claim(db, rid, worker_id, until, recovered):
            db.commit()
            row = db.get(BayesRun, rid)
            return row
    return None

def _atomic_claim(db, rid, worker_id, until, recovered):
    # 条件更新：仅当任务仍排队、或运行中且租约已过期时才领取
    if recovered:
        cond = or_(BayesRun.status == "queued",
                   (BayesRun.status == "running") & (BayesRun.lease_until < now()))
    else:
        cond = BayesRun.status == "queued"
    stmt = (update(BayesRun)
            .where(BayesRun.id == rid, cond)
            .values(status="running", lease_owner=worker_id, lease_until=until,
                    attempts=BayesRun.attempts + 1,
                    recoveries=BayesRun.recoveries + (1 if recovered else 0)))
    result = db.execute(stmt)
    if result.rowcount != 1:
        db.rollback()
        return False
    row = db.get(BayesRun, rid)
    if recovered:
        log(db, "worker:" + worker_id, "bayes.recover.lease", rid)
    row.progress = {"stage": "running", "total": (row.progress or {}).get("total", 0),
                    "completed": (row.progress or {}).get("completed", 0),
                    "detail": (f"租约超时，由{worker_id}接管中断续算" if recovered else "worker领取排队任务"),
                    "updated_at": now()}
    return True

def renew_lease(db, run_id, worker_id):
    """续约；返回False表示租约已被他人接管，调用方必须停止。"""
    row = db.get(BayesRun, run_id)
    if not row or row.lease_owner != worker_id or row.status != "running":
        return False
    row.lease_until = (parse(now()) + timedelta(seconds=LEASE_TTL_SECONDS)).isoformat()
    db.commit()
    return True

def update_progress(db, run_id, worker_id, stage, total, completed, detail):
    row = db.get(BayesRun, run_id)
    if not row or row.lease_owner != worker_id or row.status != "running":
        return False
    row.progress = {"stage": stage, "total": int(total), "completed": int(completed),
                    "detail": detail, "updated_at": now()}
    db.commit()
    return True

# —— 发布：条件更新，迟到实例无法覆盖 ——
def publish_run(db, run_id, worker_id, result):
    """完整结果原子写盘后条件落库；只有当前租约持有者能发布。"""
    row = db.get(BayesRun, run_id)
    if not row:
        raise segment_service.StaleLeaseError(f"run {run_id} 已不存在")
    if row.lease_owner != worker_id or row.status != "running":
        raise segment_service.StaleLeaseError(f"run {run_id} 租约已移交，迟到实例结果被拒绝")
    segment_service.save_final_result(run_id, result)  # 先原子写完整结果文件
    total = len(result.get("lots", []))
    stmt = (update(BayesRun)
            .where(BayesRun.id == run_id, BayesRun.status == "running",
                   BayesRun.lease_owner == worker_id)
            .values(result=result, status="completed", error="",
                    lease_owner=None, lease_until=None, dedup_key=None,
                    progress={"stage": "completed", "total": total, "completed": total,
                              "detail": "完整结果已发布", "updated_at": now()}))
    if db.execute(stmt).rowcount != 1:
        db.rollback()
        raise segment_service.StaleLeaseError(f"run {run_id} 条件发布未命中，迟到结果被拒绝")
    db.commit()
    log(db, "worker:" + worker_id, "bayes.completed", run_id)
    return db.get(BayesRun, run_id)

def fail_run(db, run_id, worker_id, message):
    row = db.get(BayesRun, run_id)
    if not row or row.lease_owner != worker_id or row.status != "running":
        logger.warning("late failure for run %s by %s ignored", run_id, worker_id)
        return None
    stmt = (update(BayesRun)
            .where(BayesRun.id == run_id, BayesRun.status == "running",
                   BayesRun.lease_owner == worker_id)
            .values(status="failed", error=message, lease_owner=None, lease_until=None,
                    dedup_key=None,
                    progress={**(row.progress or {}), "stage": "failed",
                              "detail": "计算失败：" + message, "updated_at": now()}))
    if db.execute(stmt).rowcount != 1:
        db.rollback(); return None
    db.commit()
    log(db, "worker:" + worker_id, "bayes.failed", run_id)
    return db.get(BayesRun, run_id)

# —— 兜底扫描：通知丢失/崩溃后的自动恢复 ——
def sweep(db):
    """根据数据库事实恢复：重投排队通知、暴露超时运行、回填孤立完整结果。返回处理数。"""
    recovered = 0
    queued = db.query(BayesRun).filter(BayesRun.status == "queued").order_by(BayesRun.id).all()
    for row in queued:
        _notify(row.id)  # 通知丢失也能重新找到任务
    stale_running = db.query(BayesRun).filter(BayesRun.status == "running").all()
    for row in stale_running:
        # 仅在租约失效后才考虑回填，避免与正在发布的存活worker竞争；
        # “文件已写状态未落库”的崩溃会在租约超时后由这里恢复
        if lease_expired(row.lease_until):
            orphan = segment_service.load_final_result(row.id)
            if orphan is not None and _result_matches(row, orphan):
                row.result = orphan; row.status = "completed"; row.error = ""
                row.lease_owner = None; row.lease_until = None; row.dedup_key = None
                row.recoveries = (row.recoveries or 0) + 1
                row.progress = {"stage": "completed", "total": len(orphan.get("lots", [])),
                                "completed": len(orphan.get("lots", [])),
                                "detail": "结果文件已存在，状态由扫描回填", "updated_at": now()}
                db.commit(); log(db, "sweeper", "bayes.recover.orphan-result", row.id)
                recovered += 1; continue
            # 超时任务：刷新提示并确保唤醒；实际接管的恢复计数在领取时增加，避免重复计数
            row.progress = {**(row.progress or {}), "detail": "租约超时，等待接管续算",
                            "updated_at": now()}
            db.commit(); _notify(row.id)
            log(db, "sweeper", "bayes.recover.timeout", row.id)
            recovered += 1
    return recovered

def _result_matches(row, result):
    """孤立结果必须与该运行提交时的情景自洽，拒绝串档文件。"""
    try:
        snap = row.input_snapshot
        lots, counts, _, _ = prepare_snapshot(snap)
    except Exception:
        return False
    from .analysis_service import build_hashes
    hashes = build_hashes(row.input_snapshot, lots, counts, row.threshold, row.future_n)
    return result.get("input_hash") == hashes["input_hash"] and result.get("fit_hash") == hashes["fit_hash"] \
        and len(result.get("lots", [])) == len(lots)

def export_run(db, rid, kind="summary"):
    row = get_run(db, rid)
    # 半份/未完成结果不供下载；且仍须获批
    if row.status != "completed" or not isinstance(row.result, dict) or not row.result.get("lots"):
        raise HTTPException(409, "完整结果尚未发布，暂不可查看或下载")
    if not db.query(SamplingNote).filter_by(run_id=rid, status="approved").first():
        raise HTTPException(403, "仅获批完成研究可导出")
    o = io.StringIO(); w = csv.writer(o); r = row.result; fit = r["fit"]
    if kind == "predictive":
        w.writerow(["lot", "future_n", "future_k", "conditional_probability", "fit_alpha", "fit_beta", "weak_identification", "input_hash"])
        for x in r["lots"]:
            for p in x["prediction"]["pmf"]:
                w.writerow([x["lot"]["code"], row.future_n, p["k"], p["probability"], fit["alpha"], fit["beta"], fit["weak_identification"], r["input_hash"]])
    else:
        w.writerow(["lot", "n", "k", "raw_rate", "post_alpha", "post_beta", "conditional_mean", "conditional_q025", "conditional_q975", "Pr_p_gt_threshold", "threshold", "future_n", "future_mean", "future_variance", "fit_alpha", "fit_beta", "weak_identification", "fit_hash", "input_hash"])
        for x in r["lots"]:
            w.writerow([x["lot"]["code"], x["count"]["n"], x["count"]["k"], x["raw_rate"], x["post_alpha"], x["post_beta"], x["mean"], x["lower"], x["upper"], x["prob_gt_threshold"], row.threshold, row.future_n, x["prediction"]["mean"], x["prediction"]["variance"], fit["alpha"], fit["beta"], fit["weak_identification"], r["fit_hash"], r["input_hash"]])
    return o.getvalue()
