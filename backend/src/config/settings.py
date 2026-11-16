import os
DATABASE_URL=os.environ.get("DATABASE_URL","postgresql+psycopg://germination:germination@db/germination")
REDIS_URL=os.environ.get("REDIS_URL","redis://redis:6379/0")
JWT_SECRET=os.environ.get("JWT_SECRET","local-germination-bayes-change-this-key")
SEED_DEMO=os.environ.get("SEED_DEMO","true").lower()=="true"
# 分段结果、检查点与完整结果JSON的持久化目录（backend/worker共享命名卷）
ARTIFACT_DIR=os.environ.get("ARTIFACT_DIR","/artifacts")
# 租约心跳：worker计算期间续约间隔，超过TTL未续约即视为中断可接管
LEASE_TTL_SECONDS=int(os.environ.get("LEASE_TTL_SECONDS","30"))
# 兜底扫描间隔：通知丢失/崩溃后由数据库扫描自动恢复排队与计算
SWEEP_INTERVAL_SECONDS=int(os.environ.get("SWEEP_INTERVAL_SECONDS","5"))
# 分段续算每批提交进度的步长
PROGRESS_BATCH_STEP=int(os.environ.get("PROGRESS_BATCH_STEP","1"))
