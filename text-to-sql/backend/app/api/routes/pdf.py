from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from app.services.pdf_service import PDFCohortService
import logging
import uuid
import asyncio

router = APIRouter()
logger = logging.getLogger(__name__)

# 임시 작업 결과 저장소
# 메모리 기반 저장소이므로 서버 재시작 시 초기화됩니다. 
# 프로덕션 환경에서는 Redis나 DB를 사용하는 것이 좋습니다.
jobs = {}

async def process_pdf_task(task_id: str, content: bytes, relax_mode: bool = False, deterministic: bool = True, reuse_existing: bool = True):
    """
    백그라운드에서 실행되는 PDF 처리 작업
    """
    try:
        logger.info(f"Starting PDF processing for task {task_id} (Relax: {relax_mode}, Deterministic: {deterministic}, Reuse: {reuse_existing})")
        jobs[task_id] = {"status": "processing", "message": "분석 중..."}
        
        service = PDFCohortService()
        result = await service.analyze_and_generate_sql(content, relax_mode=relax_mode, deterministic=deterministic, reuse_existing=reuse_existing)
        
        jobs[task_id] = {
            "status": "completed", 
            "result": result,
            "message": "분석 완료"
        }
        logger.info(f"Completed PDF processing for task {task_id}")
        
    except Exception as e:
        logger.error(f"Error processing PDF task {task_id}: {e}")
        jobs[task_id] = {
            "status": "failed", 
            "error": str(e),
            "message": "분석 실패"
        }

@router.post("/upload")
async def upload_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    relax_mode: bool = False,
    deterministic: bool = True,
    reuse_existing: bool = True
):
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    try:
        content = await file.read()
        if not content:
             raise HTTPException(status_code=400, detail="Empty file")

        import hashlib
        file_hash = hashlib.sha256(content).hexdigest()
        task_id = str(uuid.uuid4())
        
        # 초기 상태 설정
        jobs[task_id] = {
            "status": "pending",
            "message": "대기 중...",
            "pdf_hash": file_hash
        }
        
        # 백그라운드 작업 등록
        background_tasks.add_task(process_pdf_task, task_id, content, relax_mode, deterministic, reuse_existing)
        
        return {
            "task_id": task_id,
            "status": "pending",
            "pdf_hash": file_hash,
            "message": "PDF 분석이 시작되었습니다. 잠시 후 결과를 확인해주세요."
        }

    except Exception as e:
        logger.error(f"Error initializing PDF task: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{task_id}")
async def get_task_status(task_id: str):
    """
    작업 상태 조회 엔드포인트
    """
    if task_id not in jobs:
        # 작업이 없으면 404 반환
        raise HTTPException(status_code=404, detail="Task not found")
    
    return jobs[task_id]
