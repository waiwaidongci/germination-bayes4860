import {Run} from '../types/domain';import {ProgressStage} from '../constants/enums';
export function RunProgressPanel({run}:{run:Run}){
  const p=run.progress||{stage:run.status,total:0,completed:0,detail:''};
  const pct=p.total>0?Math.round((p.completed/p.total)*100):(run.status==='queued'?0:run.status==='running'?5:100);
  const stageLabel=ProgressStage[p.stage as keyof typeof ProgressStage]||p.stage||'';
  return <div className="run-progress">
    <div className="progress-head">
      <span className="eyebrow">{stageLabel||'运行状态'}</span>
      <div className="recovery-meta">
        <span title="累计领取/接管次数">领取 {run.attempts??0}</span>
        <span title="自动恢复次数：超时接管、丢失通知重投、孤立结果回填" className={run.recoveries?'warn':''}>恢复 {run.recoveries??0}</span>
      </div>
    </div>
    <div className="progress-track" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100} aria-label={stageLabel}>
      <i style={{width:`${pct}%`}} className={run.status==='failed'?'failed':''}/>
    </div>
    <p className="progress-detail">
      {p.total>0?`${p.completed}/${p.total} 批 · ${pct}% · `:''}{p.detail||(run.status==='queued'?'已排队，等待worker领取':'分段续算中，崩溃后可从检查点继续')}
    </p>
    {run.status==='failed'&&<p className="progress-error"><b>失败原因</b>{run.error||'计算未完成'}</p>}
    {run.recoveries>0&&<p className="recovery-note">该研究经历 {run.recoveries} 次自动恢复（租约超时接管/通知重投/结果回填），续算沿用提交时计数与情景。</p>}
  </div>;
}
