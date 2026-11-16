import os
import tempfile
import fakeredis
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from src.config.database import Base
from src.models import GerminationStudy, SeedLot, LotCount, User
from src.services import run_service
from src.utils.security import hash_password

@pytest.fixture()
def artifacts_dir(tmp_path, monkeypatch):
    d = str(tmp_path / "artifacts")
    os.makedirs(d, exist_ok=True)
    monkeypatch.setenv("ARTIFACT_DIR", d)
    from src.services import segment_service
    monkeypatch.setattr(segment_service, "ARTIFACT_DIR", d)
    yield d

@pytest.fixture()
def db_engine(tmp_path):
    path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{path}", connect_args={"timeout": 30})
    # WAL 允许并发连接在写锁上串行等待，模拟多worker领取竞争
    @event.listens_for(engine, "connect")
    def _wal(dbapi_conn, _):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=30000")
        cur.close()
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()

@pytest.fixture()
def Session(db_engine):
    return sessionmaker(bind=db_engine, expire_on_commit=False)

@pytest.fixture()
def db(Session):
    with Session() as s:
        yield s

@pytest.fixture(autouse=True)
def _patch_worker_sessions(Session, monkeypatch):
    # worker 模块在导入时绑定全局 SessionLocal（默认指向Postgres）；测试统一改用SQLite工厂
    from src import worker
    monkeypatch.setattr(worker, "SessionLocal", Session)
    yield

@pytest.fixture(autouse=True)
def fake_queue(monkeypatch):
    q = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(run_service, "queue", q)
    return q

@pytest.fixture()
def user(db):
    u = User(email="analyst@duocen.demo", name="许禾", role="analyst", password_hash=hash_password("x"))
    db.add(u); db.commit()
    return u

def build_study(db, code="DC-T", nvals=None, kvals=None, lot_count=10):
    if nvals is None:
        nvals = [40, 60, 80, 50, 70, 90, 30, 55, 45, 65][:lot_count]
    if kvals is None:
        kvals = [20, 30, 55, 25, 40, 60, 10, 30, 22, 40][:lot_count]
    st = GerminationStudy(code=code, name="韧性测试研究", description="d", source="synthetic.csv")
    db.add(st); db.flush()
    for i, (n, k) in enumerate(zip(nvals, kvals)):
        lot = SeedLot(study_id=st.id, code=f"{code}-L{i:02d}", name=f"批次{i}", origin="试验室")
        db.add(lot); db.flush()
        db.add(LotCount(lot_id=lot.id, n=int(n), k=int(k), note="计数"))
    db.commit()
    return st

@pytest.fixture()
def study(db):
    return build_study(db)

@pytest.fixture()
def make_study(db):
    def _make(code="DC-T", nvals=None, kvals=None, lot_count=10):
        return build_study(db, code=code, nvals=nvals, kvals=kvals, lot_count=lot_count)
    return _make

@pytest.fixture()
def make_input():
    from src.schemas.run import RunInput
    def _make(study_id, threshold=0.7, future_n=20, name="韧性研究"):
        return RunInput(name=name, study_id=study_id, threshold=threshold, future_n=future_n)
    return _make

@pytest.fixture()
def client(Session, db_engine, user, monkeypatch):
    # 让应用lifespan与/health使用测试SQLite引擎，避免连接默认Postgres；关闭演示seed
    from fastapi.testclient import TestClient
    from src.config import database as db_cfg
    from src.config.database import get_db
    from src.main import app
    from src import main as main_mod
    monkeypatch.setattr(db_cfg, "engine", db_engine)
    monkeypatch.setattr(main_mod, "engine", db_engine)
    monkeypatch.setattr(main_mod, "SEED_DEMO", False)
    def _get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()
    app.dependency_overrides[get_db] = _get_db
    from src.controllers.dependencies import current_user
    app.dependency_overrides[current_user] = lambda: user
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
