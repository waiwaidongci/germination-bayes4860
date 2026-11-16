"""HTTP 层集成测试：半份结果不暴露、去重复用、失败原因与进度字段随接口返回。"""
import pytest
from fastapi.testclient import TestClient
from src.config.database import get_db
from src.main import app
from src.models import BayesRun
from src.services import run_service as rs
from src.services import segment_service as seg


def _open_session(client):
    return next(client.app.dependency_overrides[get_db]())


def test_api_hides_partial_result_and_shows_recovery_fields(client, make_input, study, artifacts_dir):
    payload = make_input(study.id).model_dump()
    r = client.post("/runs", json=payload)
    assert r.status_code == 202
    body = r.json()
    rid = body["id"]
    # 排队态：result为空，但韧性字段齐全
    assert body["result"] == {} and body["progress"]["stage"] == "queued"
    assert body["attempts"] == 0 and body["recoveries"] == 0
    # 重提不另建
    r2 = client.post("/runs", json=payload)
    assert r2.status_code == 202 and r2.json()["id"] == rid and r2.json()["reused"] is True
    # 领取进入运行态：列表/详情都不泄漏半份结果
    d = _open_session(client)
    rs.claim_next_run(d, "w1"); d.close()
    target = next(x for x in client.get("/runs").json() if x["id"] == rid)
    assert target["status"] == "running" and target["result"] == {}
    detail = client.get(f"/runs/{rid}").json()
    assert detail["lease_owner"] == "w1" and detail["result"] == {}
    # 完成后结果可见且完整
    d = _open_session(client)
    row = d.get(BayesRun, rid)
    result = seg.run_segmented(rid, row.input_snapshot, row.threshold, row.future_n)
    rs.publish_run(d, rid, "w1", result); d.close()
    done = client.get(f"/runs/{rid}").json()
    assert done["status"] == "completed" and len(done["result"]["lots"]) == 10
    # 未获批导出403
    assert client.get(f"/runs/{rid}/export").status_code == 403


def test_api_completed_legacy_run_is_readable(client, db, study, artifacts_dir):
    """旧研究（韧性字段取默认值）经接口正常读取。"""
    from src.services.study_service import create_snapshot
    snap = create_snapshot(db, study)
    result = seg.run_segmented(555, snap, 0.7, 20)
    db.add(BayesRun(name="旧研究接口可读", study_id=study.id, threshold=0.7, future_n=20,
                    input_snapshot=snap, status="completed", result=result, owner="老分析员",
                    dedup_key=None, lease_owner=None, lease_until=None,
                    attempts=0, recoveries=0, progress={}))
    db.commit()
    legacy = next(x for x in client.get("/runs").json() if x["name"] == "旧研究接口可读")
    assert legacy["status"] == "completed" and legacy["result"]["fit_hash"]
