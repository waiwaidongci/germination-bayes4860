"""韧性worker：领取-续约-分段续算-条件发布，并周期性扫描恢复。

- 每次循环先尝试数据库领取（排队优先、超时接管），无任务时阻塞等待Redis通知；
- 计算期间后台线程按 TTL/3 续约，租约丢失立即停止，迟到实例不发布；
- 先验拟合/逐批预测分段落盘，崩溃后接管者从检查点续算；
- 周期性 sweep 兜底通知丢失、超时任务与“文件已写状态未落库”。
"""
import json, threading, time, traceback, uuid
from pathlib import Path
from .config.database import Base, engine, SessionLocal
from .config.settings import ARTIFACT_DIR, LEASE_TTL_SECONDS, SWEEP_INTERVAL_SECONDS
from .constants.status import RunStatus
from .models import BayesRun
from .services import run_service, segment_service
from .services.audit_service import log

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"

class LeaseGuard:
    """后台续约；stop后或租约失效时alive()为False，驱动计算尽快中止。"""
    def __init__(self, run_id, worker_id):
        self.run_id = run_id; self.worker_id = worker_id
        self._stop = threading.Event(); self._alive = True
        self.interval = max(0.2, LEASE_TTL_SECONDS / 3)
        self.thread = threading.Thread(target=self._loop, name=f"lease-{run_id}", daemon=True)
    def start(self):
        self.thread.start()
    def _loop(self):
        while not self._stop.wait(self.interval):
            try:
                with SessionLocal() as db:
                    if not run_service.renew_lease(db, self.run_id, self.worker_id):
                        self._alive = False; return
            except Exception:
                traceback.print_exc()
    def alive(self):
        return self._alive and not self._stop.is_set()
    def stop(self):
        self._stop.set()

class Progress:
    def __init__(self, run_id, worker_id):
        self.run_id = run_id; self.worker_id = worker_id
    def update(self, stage, total, completed, detail):
        with SessionLocal() as db:
            run_service.update_progress(db, self.run_id, self.worker_id, stage, total, completed, detail)

def execute_run(run_id, worker_id=WORKER_ID):
    """执行单个运行（分段续算+租约+条件发布）。供worker循环与测试复用。"""
    with SessionLocal() as db:
        row = db.get(BayesRun, run_id)
        if not row or row.status != RunStatus.RUNNING or row.lease_owner != worker_id:
            return "skipped"
        snapshot, threshold, future_n = row.input_snapshot, row.threshold, row.future_n
    guard = LeaseGuard(run_id, worker_id); guard.start()
    try:
        # 状态未落库但完整结果文件已存在：校验自洽后直接回填，避免重复计算
        with SessionLocal() as db:
            row = db.get(BayesRun, run_id)
            orphan = segment_service.load_final_result(run_id)
        if orphan is not None and run_service._result_matches(row, orphan):
            with SessionLocal() as db:
                run_service.publish_run(db, run_id, worker_id, orphan)
            return "recovered-result"
        result = segment_service.run_segmented(
            run_id, snapshot, threshold, future_n,
            progress=Progress(run_id, worker_id), lease_alive=guard.alive)
        with SessionLocal() as db:
            run_service.publish_run(db, run_id, worker_id, result)
        return "completed"
    except segment_service.StaleLeaseError as e:
        # 已被接管：本实例安静退场，绝不覆盖
        print(f"[worker] {worker_id} 放弃 run {run_id}: {e}")
        return "lease-lost"
    except Exception as e:
        traceback.print_exc()
        with SessionLocal() as db:
            run_service.fail_run(db, run_id, worker_id, str(e))
        return "failed"
    finally:
        guard.stop()

def tick(worker_id=WORKER_ID):
    """一次处理节拍：先数据库领取，无任务则等通知；末尾按需扫描。返回处理结果。"""
    Path(ARTIFACT_DIR).mkdir(parents=True, exist_ok=True)
    Path(ARTIFACT_DIR, "heartbeat").write_text(str(time.time()))
    with SessionLocal() as db:
        row = run_service.claim_next_run(db, worker_id)
    if row is None:
        notified = run_service.wait_notification(timeout=2)
        if notified is not None:
            return "notified:" + str(notified)
        with SessionLocal() as db:
            run_service.sweep(db)
        return "swept"
    return execute_run(row.id, worker_id)

def main():
    Base.metadata.create_all(engine)
    Path(ARTIFACT_DIR).mkdir(parents=True, exist_ok=True)
    last_sweep = 0.0
    print(f"[worker] {WORKER_ID} started, lease_ttl={LEASE_TTL_SECONDS}s sweep={SWEEP_INTERVAL_SECONDS}s")
    while True:
        try:
            tick(WORKER_ID)
        except Exception:
            traceback.print_exc(); time.sleep(1)
        if time.time() - last_sweep >= SWEEP_INTERVAL_SECONDS:
            try:
                with SessionLocal() as db:
                    run_service.sweep(db)
            except Exception:
                traceback.print_exc()
            last_sweep = time.time()

if __name__ == "__main__":
    main()
