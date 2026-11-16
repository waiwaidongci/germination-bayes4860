from sqlalchemy import Text,JSON,ForeignKey,Double,Integer,Index,text
from sqlalchemy.orm import Mapped,mapped_column
from ..config.database import Base
from ..utils.clock import now
class BayesRun(Base):
    __tablename__="bayes_runs"
    # 仅对未结束运行的情景去重键保持唯一；终态置空dedup_key以释放约束
    __table_args__=(
        Index("uq_bayes_runs_active_dedup","dedup_key",unique=True,
              postgresql_where=text("dedup_key IS NOT NULL"),
              sqlite_where=text("dedup_key IS NOT NULL")),
    )
    id:Mapped[int]=mapped_column(primary_key=True)
    name:Mapped[str]
    study_id:Mapped[int]=mapped_column(ForeignKey("germination_studies.id"))
    threshold:Mapped[float]=mapped_column(Double)
    future_n:Mapped[int]
    input_snapshot:Mapped[dict]=mapped_column(JSON)
    status:Mapped[str]=mapped_column(default="queued")
    result:Mapped[dict]=mapped_column(JSON,default=dict)
    error:Mapped[str]=mapped_column(Text,default="")
    owner:Mapped[str]
    created_at:Mapped[str]=mapped_column(default=now)
    # —— 韧性运行：去重、租约、恢复与可观测性 ——
    # 同研究同情景在排队/计算中的去重键；进入终态后置空以释放约束
    dedup_key:Mapped[str]=mapped_column(Text,nullable=True,default=None,index=True)
    # 持有租约的worker；None表示无人领取
    lease_owner:Mapped[str]=mapped_column(Text,nullable=True,default=None)
    # 租约到期时间（UTC ISO），到期可被其他worker接管
    lease_until:Mapped[str]=mapped_column(Text,nullable=True,default=None)
    # 累计领取/接管次数（首次领取与每次超时接管均计数）
    attempts:Mapped[int]=mapped_column(Integer,default=0)
    # 自动恢复次数（超时接管、丢失通知重投、孤立结果回填）
    recoveries:Mapped[int]=mapped_column(Integer,default=0)
    # 分段进度：{stage,total,completed,detail,updated_at}
    progress:Mapped[dict]=mapped_column(JSON,default=dict)
