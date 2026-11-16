from typing import Literal
from fastapi import APIRouter,Depends
from fastapi.responses import Response
from sqlalchemy.orm import Session
from ..config.database import get_db
from ..controllers.dependencies import current_user,roles
from ..constants.roles import Role
from ..models import BayesRun
from ..schemas.run import RunInput
from ..services.run_service import get_run,create_run,export_run
from ..utils.serialize import record
router=APIRouter(prefix='/runs',tags=['runs'])
def visible(row):
    # 半份/未完成结果不供查看：未完成运行只暴露状态、进度、恢复次数与失败原因
    data=record(row)
    if row.status!='completed' or not isinstance(row.result,dict) or not row.result.get('lots'):
        data['result']={}
    return data
@router.get('')
def index(db:Session=Depends(get_db),user=Depends(current_user)):
    return [visible(r) for r in db.query(BayesRun).order_by(BayesRun.id.desc())]
@router.get('/{rid}')
def show(rid:int,db:Session=Depends(get_db),user=Depends(current_user)):return visible(get_run(db,rid))
@router.post('',status_code=202)
def create(data:RunInput,db:Session=Depends(get_db),user=Depends(roles(Role.ADMIN,Role.ANALYST))):return create_run(db,data,user)
@router.get('/{rid}/export')
def export(rid:int,kind:Literal['summary','predictive']='summary',db:Session=Depends(get_db),user=Depends(current_user)):return Response(content='﻿'+export_run(db,rid,kind),media_type='text/csv',headers={'Content-Disposition':f'attachment; filename="germination-{rid}-{kind}.csv"'})
