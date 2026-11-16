"""分段可续算的萌发分析流水线。

两个可独立续算的阶段：
1. 先验拟合（fit segment）——多起点边际MLE，结果带输入计数指纹；
2. 逐批条件后验与预测（lot segments）——每批一个检查点，已完成批次不重算。

所有检查点都嵌入提交时计数/情景指纹与算法版本：来源（n/k、阈值、未来m、算法版本）
与检查点不一致时判定失效并重算，来源变动不会混入续算。
检查点写盘采用临时文件+原子rename，损坏（无法解析/缺字段/指纹不符）自动重算。
"""
import json, os, tempfile
from pathlib import Path
from .eb_algorithm import fit_hyperparameters
from .posterior_algorithm import posterior_prediction
from .analysis_service import prepare_snapshot, VERSION, build_hashes, assemble_result
from ..utils.fingerprint import fingerprint
from ..config.settings import ARTIFACT_DIR

class StaleLeaseError(RuntimeError):
    """租约已被接管，当前实例必须停止，避免迟到实例覆盖结果。"""

def run_dir(run_id):
    base=Path(ARTIFACT_DIR)/f"run-{run_id}"
    base.mkdir(parents=True,exist_ok=True)
    return base
def fit_path(run_id):return run_dir(run_id)/"fit.json"
def lot_path(run_id,index):return run_dir(run_id)/f"lot-{index:04d}.json"
def result_path(run_id):return Path(ARTIFACT_DIR)/f"run-{run_id}.json"
def _atomic_write(path,payload):
    path=Path(path);fd,tmp=tempfile.mkstemp(prefix=path.name+".",suffix=".tmp",dir=str(path.parent))
    try:
        with os.fdopen(fd,"w",encoding="utf-8") as f:
            json.dump(payload,f,ensure_ascii=False,allow_nan=False)
            f.flush();os.fsync(f.fileno())
        os.replace(tmp,path)
    except Exception:
        try:os.unlink(tmp)
        except OSError:pass
        raise
def _read_json(path):
    try:
        with open(path,encoding="utf-8") as f:return json.load(f)
    except (OSError,ValueError,TypeError):return None

def load_fit_segment(run_id,expected):
    seg=_read_json(fit_path(run_id))
    if not seg or seg.get("kind")!="fit":return None
    if seg.get("algorithm_version")!=VERSION:return None
    if seg.get("input_hash")!=expected["input_hash"] or seg.get("fit_hash")!=expected["fit_hash"]:return None
    fit=seg.get("fit")
    if not isinstance(fit,dict) or not all(key in fit for key in ("alpha","beta","attempts")):return None
    return fit

def save_fit_segment(run_id,fit,expected):
    _atomic_write(fit_path(run_id),{"kind":"fit","algorithm_version":VERSION,"input_hash":expected["input_hash"],"fit_hash":expected["fit_hash"],"fit":fit})

def load_lot_segment(run_id,index,expected,lot_code):
    seg=_read_json(lot_path(run_id,index))
    if not seg or seg.get("kind")!="lot" or seg.get("index")!=index:return None
    if seg.get("algorithm_version")!=VERSION:return None
    if seg.get("input_hash")!=expected["input_hash"] or seg.get("fit_hash")!=expected["fit_hash"]:return None
    if seg.get("lot_code")!=lot_code:return None
    row=seg.get("row")
    if not isinstance(row,dict) or "post_alpha" not in row or "prediction" not in row:return None
    return row

def save_lot_segment(run_id,index,lot_code,row,expected):
    _atomic_write(lot_path(run_id,index),{"kind":"lot","index":index,"lot_code":lot_code,"algorithm_version":VERSION,"input_hash":expected["input_hash"],"fit_hash":expected["fit_hash"],"row":row})

def load_final_result(run_id):
    """读取完整结果文件；不存在或损坏返回None（半份结果不供查看）。"""
    data=_read_json(result_path(run_id))
    if not isinstance(data,dict):return None
    if data.get("algorithm_version")!=VERSION:return None
    if not all(key in data for key in ("fit","lots","fit_hash","input_hash")):return None
    if not isinstance(data.get("lots"),list) or not data["lots"]:return None
    return data

def save_final_result(run_id,result):
    _atomic_write(result_path(run_id),result)

def run_segmented(run_id,snapshot,threshold,future_n,progress=None,lease_alive=None):
    """执行分段流水线，返回与 analyze_germination 同构的完整结果。

    progress(dict-like,可选)：含 update(stage,total,completed,detail) 回调。
    lease_alive(callable,可选)：返回False时抛 StaleLeaseError 立即停止。
    """
    def check_lease():
        if lease_alive is not None and not lease_alive():
            raise StaleLeaseError(f"run {run_id} 租约已失效，停止计算以避免迟到覆盖")
    lots,counts,n,k=prepare_snapshot(snapshot)
    expected=build_hashes(snapshot,lots,counts,threshold,future_n)
    # —— 阶段1：先验拟合（沿用提交时计数；检查点命中则整段跳过）——
    check_lease()
    fit=load_fit_segment(run_id,expected)
    if fit is not None:
        if progress is not None:progress.update("fit",1,1,"沿用已完成先验拟合检查点")
    else:
        if progress is not None:progress.update("fit",1,0,"多起点边际似然拟合")
        fit=fit_hyperparameters(n,k);check_lease();save_fit_segment(run_id,fit,expected)
        if progress is not None:progress.update("fit",1,1,"先验拟合检查点已落盘")
    # —— 阶段2：逐批条件后验与同批预测，逐批续算 ——
    total=len(lots);rows=[]
    for index,(lot,count) in enumerate(zip(lots,counts)):
        check_lease()
        row=load_lot_segment(run_id,index,expected,lot["code"])
        if row is not None:
            rows.append(row)
        else:
            computed={"lot":lot,"count":count,**posterior_prediction(fit["alpha"],fit["beta"],count["n"],count["k"],threshold,future_n)}
            check_lease();save_lot_segment(run_id,index,lot["code"],computed,expected);rows.append(computed)
        if progress is not None:
            progress.update("lots",total,index+1,f"逐批条件后验与预测 {index+1}/{total}")
    check_lease()
    result=assemble_result(snapshot,lots,counts,fit,rows,threshold,future_n)
    # 组装结果必须与检查点指纹自洽，防止来源串入
    if result["fit_hash"]!=expected["fit_hash"] or result["input_hash"]!=expected["input_hash"]:
        raise StaleLeaseError("分段指纹与组装结果不一致，拒绝发布")
    return result
