"""排队恢复、并发领取、超时接管与迟到实例去重测试。

覆盖：通知丢失靠扫描找到任务、并发领取只有一个worker成功、租约超时被接管、
迟到实例的结果无法覆盖、同一研究只发布一份完整结果、失败原因与恢复次数可观测。
"""
import threading
from datetime import datetime, timezone, timedelta
import pytest
from src.models import BayesRun
from src.services import run_service as rs
from src.services import segment_service as seg
from src.utils.clock import iso


def _enqueue(db, payload, study, user):
    return rs.create_run(db, payload, user)


def test_lost_notification_recovered_by_sweep(db, make_input, study, user, fake_queue, artifacts_dir):
    """Redis通知丢失（列表为空）：扫描重新投递，任务仍能被找到并完成。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    fake_queue.flushdb()  # 通知彻底丢失
    assert fake_queue.llen(rs.JOB_KEY) == 0
    # 扫描不依赖通知，直接按数据库事实重投
    rs.sweep(db)
    assert fake_queue.llen(rs.JOB_KEY) == 1
    row = rs.claim_next_run(db, "w1")
    assert row is not None and row.id == rid


def test_concurrent_claim_single_winner(db, make_input, study, user, Session, artifacts_dir):
    """多worker并发领取同一排队任务：恰有一个成功，且attempts只增加一次。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    winners, errors = [], []
    barrier = threading.Barrier(5)

    def claim(worker):
        s = Session()
        try:
            barrier.wait()
            row = rs.claim_next_run(s, worker)
            if row is not None:
                winners.append((worker, row.id))
        except Exception as e:  # 锁竞争等异常不应产生第二个领取者
            errors.append(e)
        finally:
            s.close()
    threads = [threading.Thread(target=claim, args=(f"w{i}",)) for i in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert errors == [], errors
    assert len(winners) == 1 and winners[0][1] == rid
    row = db.get(BayesRun, rid)
    assert row.attempts == 1 and row.status == "running"


def test_lease_timeout_takeover_and_resume(db, make_input, study, user, artifacts_dir):
    """租约超时后另一worker接管，恢复次数增加且从检查点续算到完成。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    first = rs.claim_next_run(db, "w1")
    # w1完成先验分段后崩溃，留下未过期前的部分检查点
    snapshot, thr, fn = first.input_snapshot, first.threshold, first.future_n
    lots, counts, n, k = seg.prepare_snapshot(snapshot)
    from src.services.eb_algorithm import fit_hyperparameters
    from src.services.analysis_service import build_hashes
    expected = seg.build_hashes(snapshot, lots, counts, thr, fn)
    seg.save_fit_segment(rid, fit_hyperparameters(n, k), expected)
    # 租约超时
    first.lease_until = iso(datetime.now(timezone.utc) - timedelta(seconds=5)); db.commit()
    # 未过期时不能接管
    fresh = db.get(BayesRun, rid)
    fresh.lease_until = iso(datetime.now(timezone.utc) + timedelta(seconds=20)); db.commit()
    assert rs.claim_next_run(db, "w2") is None
    # 过期后w2接管
    fresh.lease_until = iso(datetime.now(timezone.utc) - timedelta(seconds=1)); db.commit()
    taken = rs.claim_next_run(db, "w2")
    assert taken is not None and taken.id == rid and taken.lease_owner == "w2"
    assert taken.attempts == 2 and taken.recoveries == 1
    result = seg.run_segmented(rid, snapshot, thr, fn, lease_alive=lambda: True)
    published = rs.publish_run(db, rid, "w2", result)
    assert published.status == "completed"


def test_late_instance_cannot_overwrite(db, make_input, study, user, artifacts_dir):
    """原持有者在租约被接管后迟到完成：其条件发布被拒绝，只有接管者发布一份结果。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    w1 = rs.claim_next_run(db, "w1")
    snapshot, thr, fn = w1.input_snapshot, w1.threshold, w1.future_n
    w1.lease_until = iso(datetime.now(timezone.utc) - timedelta(seconds=1)); db.commit()
    w2 = rs.claim_next_run(db, "w2")
    assert w2.lease_owner == "w2"
    # w1迟到算出一份结果（租约检查对本调用放行），但发布必须被拒
    late_result = seg.run_segmented(rid, snapshot, thr, fn, lease_alive=lambda: True)
    with pytest.raises(seg.StaleLeaseError):
        rs.publish_run(db, rid, "w1", late_result)
    # w1迟到的失败上报也被忽略
    assert rs.fail_run(db, rid, "w1", "迟到实例的失败") is None
    row = db.get(BayesRun, rid)
    assert row.status == "running" and row.lease_owner == "w2"
    # 接管者正常发布唯一一份完整结果
    good = seg.run_segmented(rid, snapshot, thr, fn, lease_alive=lambda: True)
    rs.publish_run(db, rid, "w2", good)
    done = db.get(BayesRun, rid)
    assert done.status == "completed" and done.result["input_hash"] == good["input_hash"]
    assert done.lease_owner is None


def test_duplicate_submission_single_published_result(db, make_input, study, user, fake_queue, artifacts_dir):
    """同一研究同情景多次重提只对应一个运行，最终只发布一份完整结果。"""
    r1 = _enqueue(db, make_input(study.id), study, user)
    r2 = _enqueue(db, make_input(study.id), study, user)
    r3 = _enqueue(db, make_input(study.id), study, user)
    assert r1["id"] == r2["id"] == r3["id"]
    assert r2["reused"] is True
    rid = r1["id"]
    row = rs.claim_next_run(db, "w1")
    result = seg.run_segmented(rid, row.input_snapshot, row.threshold, row.future_n)
    rs.publish_run(db, rid, "w1", result)
    active = db.query(BayesRun).filter(BayesRun.status.in_(("queued", "running"))).count()
    assert active == 0
    completed = db.query(BayesRun).filter_by(status="completed").count()
    assert completed == 1
    # 终态后去重键释放，同情景可作为新历史研究提交
    r4 = _enqueue(db, make_input(study.id), study, user)
    assert r4["reused"] is False and r4["id"] != rid


def test_different_scenario_creates_distinct_run(db, make_input, study, user, artifacts_dir):
    """同研究但阈值/未来m不同属于不同情景，不去重。"""
    r1 = _enqueue(db, make_input(study.id, threshold=0.7, future_n=20), study, user)
    r2 = _enqueue(db, make_input(study.id, threshold=0.8, future_n=20), study, user)
    r3 = _enqueue(db, make_input(study.id, threshold=0.7, future_n=50), study, user)
    assert len({r1["id"], r2["id"], r3["id"]}) == 3


def test_failure_records_reason_and_allows_resubmit(db, make_input, study, user, artifacts_dir, monkeypatch):
    """计算失败记录原因，释放去重键后允许重新提交；进度显示失败原因。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    row = rs.claim_next_run(db, "w1")
    def boom(*a, **kw):
        raise ValueError("所有起点未收敛，拒绝输出先验")
    monkeypatch.setattr(seg, "fit_hyperparameters", boom)
    outcome = None
    from src import worker
    outcome = worker.execute_run(rid, "w1")
    assert outcome == "failed"
    db.rollback()  # worker用独立会话提交；刷新本会话快照以读取已落库的失败状态
    failed = db.get(BayesRun, rid)
    assert failed.status == "failed" and "拒绝输出先验" in failed.error
    assert failed.progress["stage"] == "failed" and failed.dedup_key is None
    # 重新提交成为新运行
    again = _enqueue(db, make_input(study.id), study, user)
    assert again["id"] != rid and again["status"] == "queued"


def test_progress_observable_during_run(db, make_input, study, user, artifacts_dir):
    """进度、恢复次数与失败原因持久化在运行记录上供页面展示。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    rs.claim_next_run(db, "w1")
    rs.update_progress(db, rid, "w1", "lots", 10, 4, "逐批条件后验与预测 4/10")
    row = db.get(BayesRun, rid)
    assert row.progress == {"stage": "lots", "total": 10, "completed": 4,
                            "detail": "逐批条件后验与预测 4/10",
                            "updated_at": row.progress["updated_at"]}
    # 非持有者不能写进度
    assert rs.update_progress(db, rid, "intruder", "lots", 10, 5, "x") is False


def test_sweep_requeues_and_completes_end_to_end(db, make_input, study, user, fake_queue, artifacts_dir):
    """通知丢失+崩溃后，扫描→接管→续算→发布的完整恢复链路只产出一份结果。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    fake_queue.flushdb()
    # w1领取后崩溃：状态running、租约过期、已落先验检查点
    w1 = rs.claim_next_run(db, "w1")
    snapshot, thr, fn = w1.input_snapshot, w1.threshold, w1.future_n
    lots, counts, n, k = seg.prepare_snapshot(snapshot)
    from src.services.eb_algorithm import fit_hyperparameters
    from src.services.analysis_service import build_hashes
    expected = seg.build_hashes(snapshot, lots, counts, thr, fn)
    seg.save_fit_segment(rid, fit_hyperparameters(n, k), expected)
    w1.lease_until = iso(datetime.now(timezone.utc) - timedelta(seconds=10)); db.commit()
    rs.sweep(db)  # 通知重投+标记超时
    w2 = rs.claim_next_run(db, "w2")
    assert w2.id == rid and w2.recoveries == 1
    result = seg.run_segmented(rid, snapshot, thr, fn)
    rs.publish_run(db, rid, "w2", result)
    done = db.get(BayesRun, rid)
    assert done.status == "completed" and len(done.result["lots"]) == len(lots)
    assert db.query(BayesRun).filter_by(status="completed").count() == 1
