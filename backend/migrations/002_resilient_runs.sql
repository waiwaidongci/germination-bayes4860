-- 韧性运行：去重键、租约、恢复计数与分段进度
-- 表由SQLAlchemy metadata首次创建；本脚本为已部署库的可选升级。
ALTER TABLE bayes_runs ADD COLUMN IF NOT EXISTS dedup_key TEXT;
ALTER TABLE bayes_runs ADD COLUMN IF NOT EXISTS lease_owner TEXT;
ALTER TABLE bayes_runs ADD COLUMN IF NOT EXISTS lease_until TEXT;
ALTER TABLE bayes_runs ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE bayes_runs ADD COLUMN IF NOT EXISTS recoveries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE bayes_runs ADD COLUMN IF NOT EXISTS progress JSON NOT NULL DEFAULT '{}'::json;

-- 仅对未结束运行（queued/running）的情景去重键保持唯一；终态置空。
CREATE UNIQUE INDEX IF NOT EXISTS uq_bayes_runs_active_dedup
    ON bayes_runs (dedup_key)
    WHERE dedup_key IS NOT NULL;

-- 扫描器恢复排队/接管超时任务的工作索引
CREATE INDEX IF NOT EXISTS bayes_runs_status_idx ON bayes_runs (status);
CREATE INDEX IF NOT EXISTS bayes_runs_lease_until_idx ON bayes_runs (lease_until);
