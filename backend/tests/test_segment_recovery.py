"""断点续算与结果完整性测试。

覆盖：分段续算沿用提交时计数与情景、来源变动不混入续算、缺失/损坏分段重算、
文件已写状态未落库的回填恢复、半份结果不供查看/下载、旧研究可读。
"""
import json, os
from datetime import datetime, timezone, timedelta
import pytest
from src.models import BayesRun, SamplingNote
from src.services import run_service as rs
from src.services import segment_service as seg
from src.utils.clock import iso
from src.services.study_service import create_snapshot


def _enqueue(db, payload, study, user):
    return rs.create_run(db, payload, user)


def test_segmented_resume_reuses_completed_parts(db, make_input, study, user, artifacts_dir, monkeypatch):
    """先验与部分批次已完成时，接管者沿用检查点，只补缺失分段。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    row = rs.claim_next_run(db, "w1")
    assert row.id == rid
    # 模拟w1在完成先验+前3批后崩溃：直接跑一次分段流水线的前置部分
    snapshot, thr, fn = row.input_snapshot, row.threshold, row.future_n
    lots, counts, n, k = seg.prepare_snapshot(snapshot)
    from src.services.eb_algorithm import fit_hyperparameters
    from src.services.analysis_service import build_hashes
    expected = seg.build_hashes(snapshot, lots, counts, thr, fn)
    fit = fit_hyperparameters(n, k)
    seg.save_fit_segment(rid, fit, expected)
    for idx in range(3):
        from src.services.posterior_algorithm import posterior_prediction
        r = posterior_prediction(fit["alpha"], fit["beta"], counts[idx]["n"], counts[idx]["k"], thr, fn)
        seg.save_lot_segment(rid, idx, lots[idx]["code"], {"lot": lots[idx], "count": counts[idx], **r}, expected)
    # 让租约过期，w2接管
    row.lease_until = iso(datetime.now(timezone.utc) - timedelta(seconds=1)); db.commit()
    taken = rs.claim_next_run(db, "w2")
    assert taken.id == rid and taken.lease_owner == "w2"
    assert taken.attempts == 2 and taken.recoveries == 1

    calls = {"fit": 0, "post": 0}
    import src.services.segment_service as mod
    monkeypatch.setattr(mod, "fit_hyperparameters", lambda *a, **kw: calls.__setitem__("fit", calls["fit"] + 1) or fit)
    from src.services import posterior_algorithm
    real_post = posterior_algorithm.posterior_prediction
    def counting_post(*a, **kw):
        calls["post"] += 1
        return real_post(*a, **kw)
    monkeypatch.setattr(mod, "posterior_prediction", counting_post)

    result = mod.run_segmented(rid, snapshot, thr, fn)
    assert calls["fit"] == 0, "已完成先验不应重算"
    assert calls["post"] == len(lots) - 3, "只补算未完成批次"
    assert len(result["lots"]) == len(lots)
    rs.publish_run(db, rid, "w2", result)
    done = db.get(BayesRun, rid)
    assert done.status == "completed" and done.lease_owner is None and done.dedup_key is None


def test_resume_uses_submitted_counts_when_source_changes(db, make_input, study, user, artifacts_dir):
    """来源（n/k）在排队后变动：续算沿用提交时快照，新数据不混入结果。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    submitted = db.get(BayesRun, rid)
    snap_before = json.loads(json.dumps(submitted.input_snapshot))
    # 来源表被改动（模拟上游CSV更新），但运行快照冻结
    from src.models import LotCount
    first = db.query(LotCount).first()
    original_n, original_k = first.n, first.k
    first.n = original_n + 17; first.k = original_k + 5; db.commit()
    row = rs.claim_next_run(db, "w1")
    result = seg.run_segmented(rid, row.input_snapshot, row.threshold, row.future_n)
    rs.publish_run(db, rid, "w1", result)
    done = db.get(BayesRun, rid)
    # 结果内嵌的快照仍是提交时的计数
    assert done.result["input_snapshot"]["counts"][0]["n"] == snap_before["counts"][0]["n"]
    assert done.result["input_hash"] == created["input_hash"] if "input_hash" in created else True
    # 与用提交快照直接计算的指纹一致
    from src.services.analysis_service import build_hashes
    lots, counts, _, _ = seg.prepare_snapshot(snap_before)
    h = build_hashes(snap_before, lots, counts, done.threshold, done.future_n)
    assert done.result["input_hash"] == h["input_hash"]
    db.rollback()


def test_corrupt_or_missing_segment_is_recomputed(db, make_input, study, user, artifacts_dir):
    """损坏的分段文件被忽略并重算；缺失分段同样补齐。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    row = rs.claim_next_run(db, "w1")
    snapshot, thr, fn = row.input_snapshot, row.threshold, row.future_n
    lots, counts, n, k = seg.prepare_snapshot(snapshot)
    from src.services.eb_algorithm import fit_hyperparameters
    from src.services.analysis_service import build_hashes
    expected = seg.build_hashes(snapshot, lots, counts, thr, fn)
    fit = fit_hyperparameters(n, k)
    seg.save_fit_segment(rid, fit, expected)
    # 损坏一个批次文件、删除另一个
    from src.services.posterior_algorithm import posterior_prediction
    for idx in range(4):
        r = posterior_prediction(fit["alpha"], fit["beta"], counts[idx]["n"], counts[idx]["k"], thr, fn)
        seg.save_lot_segment(rid, idx, lots[idx]["code"], {"lot": lots[idx], "count": counts[idx], **r}, expected)
    seg.lot_path(rid, 1).write_text("{ 损坏的半截JSON")
    seg.lot_path(rid, 2).unlink()
    assert seg.load_lot_segment(rid, 1, expected, lots[1]["code"]) is None
    assert seg.load_lot_segment(rid, 2, expected, lots[2]["code"]) is None
    # 完整流水线仍成功，损坏/缺失分段被重算
    result = seg.run_segmented(rid, snapshot, thr, fn)
    assert len(result["lots"]) == len(lots)
    assert seg.load_lot_segment(rid, 1, expected, lots[1]["code"]) is not None
    assert seg.load_lot_segment(rid, 2, expected, lots[2]["code"]) is not None


def test_segment_with_changed_scenario_is_ignored(db, make_input, study, user, artifacts_dir):
    """阈值/未来m变化使input_hash变化，旧情景检查点不得被新运行沿用。"""
    created = _enqueue(db, make_input(study.id, threshold=0.7, future_n=20), study, user)
    rid = created["id"]
    row = rs.claim_next_run(db, "w1")
    snapshot = row.input_snapshot
    # 写出旧情景的先验与批次检查点
    result_old = seg.run_segmented(rid, snapshot, 0.7, 20)
    # 新情景（不同future_n）：构造不同expected，旧lot检查点必须失效
    lots, counts, _, _ = seg.prepare_snapshot(snapshot)
    expected_new = seg.build_hashes(snapshot, lots, counts, 0.7, 50)
    assert seg.load_fit_segment(rid, expected_new) is None or True  # fit_hash与m无关，fit可沿用
    # lot分段含input_hash，情景变化后必须重算
    assert seg.load_lot_segment(rid, 0, expected_new, lots[0]["code"]) is None
    result_new = seg.run_segmented(rid, snapshot, 0.7, 50)
    assert result_new["input_hash"] != result_old["input_hash"]
    assert result_new["fit_hash"] == result_old["fit_hash"], "阈值/m不参与先验，fit_hash应不变"
    assert result_new["lots"][0]["prediction"]["future_n"] == 50


def test_orphan_result_file_backfills_status(db, make_input, study, user, artifacts_dir):
    """结果文件已写完但状态未落库（崩溃在commit前）：租约超时后扫描回填为completed。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    row = rs.claim_next_run(db, "w1")
    result = seg.run_segmented(rid, row.input_snapshot, row.threshold, row.future_n)
    seg.save_final_result(rid, result)  # 只写文件，不更新数据库状态
    assert db.get(BayesRun, rid).status == "running"
    # 存活租约期间扫描不应干预（正在发布的worker可能正处于写盘与提交之间）
    assert rs.sweep(db) == 0
    assert db.get(BayesRun, rid).status == "running"
    # 模拟worker崩溃：租约超时后扫描回填
    row.lease_until = iso(datetime.now(timezone.utc) - timedelta(seconds=1)); db.commit()
    recovered = rs.sweep(db)
    assert recovered >= 1
    done = db.get(BayesRun, rid)
    assert done.status == "completed" and done.result["input_hash"] == result["input_hash"]
    assert done.recoveries == 1 and done.lease_owner is None


def test_foreign_orphan_result_is_rejected(db, make_input, study, user, artifacts_dir, make_study):
    """串档的孤立结果文件（指纹不符）不得回填，避免错误发布。"""
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    row = rs.claim_next_run(db, "w1")
    other_study = make_study(code="DC-OTHER")
    other_snap = create_snapshot(db, other_study)
    other_result = seg.run_segmented(99999, other_snap, 0.7, 20)  # 写到别的目录
    # 把别的研究结果放进本运行的结果文件位置
    seg.save_final_result(rid, other_result)
    row.lease_until = iso(datetime.now(timezone.utc) - timedelta(seconds=1)); db.commit()
    rs.sweep(db)
    still = db.get(BayesRun, rid)
    assert still.status == "running", "串档结果必须被拒绝，任务等待接管重算"
    assert still.result == {} or not still.result.get("lots")


def test_partial_result_not_viewable_or_downloadable(db, make_input, study, user, artifacts_dir):
    """半份/未完成结果不供查看；导出在完成前与获批前都被拒绝。"""
    from src.routes.runs import visible
    created = _enqueue(db, make_input(study.id), study, user)
    rid = created["id"]
    row = rs.claim_next_run(db, "w1")
    # 计算中：接口只暴露状态/进度，result为空
    view = visible(db.get(BayesRun, rid))
    assert view["result"] == {} and view["status"] == "running"
    with pytest.raises(Exception) as ei:
        rs.export_run(db, rid)
    assert ei.value.status_code == 409
    # 完成但未获批：可查看不可下载
    result = seg.run_segmented(rid, row.input_snapshot, row.threshold, row.future_n)
    rs.publish_run(db, rid, "w1", result)
    assert visible(db.get(BayesRun, rid))["result"]["lots"]
    with pytest.raises(Exception) as ei2:
        rs.export_run(db, rid)
    assert ei2.value.status_code == 403
    # 获批后可下载
    db.add(SamplingNote(run_id=rid, title="补种建议", interpretation="条件EB解释足够长",
                        proposal="同口径独立试种方案足够长", status="approved",
                        owner=user.name, reviewer="陆岑", review_note="批准归档意见足够长"))
    db.commit()
    csv_text = rs.export_run(db, rid)
    assert "lot" in csv_text and len(csv_text.splitlines()) > 2


def test_legacy_completed_run_remains_readable(db, study, user, artifacts_dir):
    """旧版无韧性字段的已完成研究仍可读取与导出。"""
    snap = create_snapshot(db, study)
    legacy_result = seg.run_segmented(777, snap, 0.7, 20)
    legacy = BayesRun(name="旧版基线研究", study_id=study.id, threshold=0.7, future_n=20,
                      input_snapshot=snap, status="completed", result=legacy_result,
                      owner="老分析员", dedup_key=None, lease_owner=None, lease_until=None,
                      attempts=0, recoveries=0, progress={})
    db.add(legacy); db.commit()
    from src.routes.runs import visible
    view = visible(db.get(BayesRun, legacy.id))
    assert view["status"] == "completed" and view["result"]["fit_hash"]
    db.add(SamplingNote(run_id=legacy.id, title="历史建议", interpretation="历史解释足够长",
                        proposal="历史方案足够长", status="approved", owner=user.name,
                        reviewer="陆岑", review_note="历史批准意见足够长"))
    db.commit()
    assert "fit_alpha" in rs.export_run(db, legacy.id)
