from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from app.services.pdf_service import PDFCohortService
from app.services.logging_store.store import append_event
from app.core.config import get_settings
import logging
import uuid
import time
from datetime import datetime
from typing import Optional

router = APIRouter()
logger = logging.getLogger(__name__)

# 임시 작업 결과 저장소
# 메모리 기반 저장소이므로 서버 재시작 시 초기화됩니다. 
# 프로덕션 환경에서는 Redis나 DB를 사용하는 것이 좋습니다.
jobs = {}

def _fmt_ts(ts: int) -> str:
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _sec(ms: int) -> float:
    return round(float(ms) / 1000.0, 3)


async def process_pdf_task(
    task_id: str,
    content: bytes,
    file_hash: str,
    filename: str,
    relax_mode: bool = False,
    deterministic: bool = True,
    reuse_existing: bool = True,
    accuracy_mode: Optional[bool] = None,
):
    """
    백그라운드에서 실행되는 PDF 처리 작업
    """
    submitted_at_ts = int(jobs.get(task_id, {}).get("submitted_at_ts") or time.time())
    started_at_ts = int(time.time())
    queue_wait_ms = max(0, int((started_at_ts - submitted_at_ts) * 1000))
    started_perf = time.perf_counter()

    try:
        logger.info(
            "Starting PDF processing for task %s (Relax: %s, Deterministic: %s, Reuse: %s, Accuracy: %s)",
            task_id,
            relax_mode,
            deterministic,
            reuse_existing,
            accuracy_mode,
        )
        jobs[task_id] = {
            "status": "processing",
            "message": "분석 중...",
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "accuracy_mode": accuracy_mode,
            "submitted_at_ts": submitted_at_ts,
            "submitted_at": _fmt_ts(submitted_at_ts),
            "started_at_ts": started_at_ts,
            "started_at": _fmt_ts(started_at_ts),
            "queue_wait_ms": queue_wait_ms,
            "queue_wait_sec": _sec(queue_wait_ms),
        }
        
        service = PDFCohortService()
        result = await service.analyze_and_generate_sql(
            content,
            filename=filename,
            relax_mode=relax_mode,
            deterministic=deterministic,
            reuse_existing=reuse_existing,
            accuracy_mode=accuracy_mode,
        )
        result_status = str(result.get("status") or "completed").strip().lower()
        job_status = "completed"
        message = "분석 완료"
        if result_status == "needs_user_input":
            job_status = "needs_user_input"
            message = "모호성 해결이 필요합니다"
        elif result_status == "validation_failed":
            job_status = "validation_failed"
            message = "검증 실패 (리포트 확인 필요)"
        elif result_status == "completed_with_ambiguities":
            job_status = "completed_with_ambiguities"
            message = "분석 완료 (모호성 포함)"

        completed_at_ts = int(time.time())
        analysis_duration_ms = max(0, int((time.perf_counter() - started_perf) * 1000))
        total_elapsed_ms = max(analysis_duration_ms, int((completed_at_ts - submitted_at_ts) * 1000))
        
        jobs[task_id] = {
            "status": job_status,
            "result": result,
            "message": message,
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "accuracy_mode": accuracy_mode,
            "submitted_at_ts": submitted_at_ts,
            "submitted_at": _fmt_ts(submitted_at_ts),
            "started_at_ts": started_at_ts,
            "started_at": _fmt_ts(started_at_ts),
            "completed_at_ts": completed_at_ts,
            "completed_at": _fmt_ts(completed_at_ts),
            "queue_wait_ms": queue_wait_ms,
            "queue_wait_sec": _sec(queue_wait_ms),
            "analysis_duration_ms": analysis_duration_ms,
            "analysis_duration_sec": _sec(analysis_duration_ms),
            "total_elapsed_ms": total_elapsed_ms,
            "total_elapsed_sec": _sec(total_elapsed_ms),
        }
        append_event(get_settings().events_log_path, {
            "type": "audit",
            "event": "pdf_analysis",
            "status": "success" if job_status in {"completed", "completed_with_ambiguities"} else job_status,
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "duration_ms": analysis_duration_ms,
            "queue_wait_ms": queue_wait_ms,
            "total_elapsed_ms": total_elapsed_ms,
            "accuracy_mode": accuracy_mode,
            "rows_returned": int(((result.get("db_result") or {}).get("row_count") or 0)),
        })
        logger.info(f"Completed PDF processing for task {task_id}")
        
    except Exception as e:
        completed_at_ts = int(time.time())
        analysis_duration_ms = max(0, int((time.perf_counter() - started_perf) * 1000))
        total_elapsed_ms = max(analysis_duration_ms, int((completed_at_ts - submitted_at_ts) * 1000))
        logger.exception("Error processing PDF task %s", task_id)
        jobs[task_id] = {
            "status": "failed", 
            "error": str(e),
            "message": "분석 실패",
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "accuracy_mode": accuracy_mode,
            "submitted_at_ts": submitted_at_ts,
            "submitted_at": _fmt_ts(submitted_at_ts),
            "started_at_ts": started_at_ts,
            "started_at": _fmt_ts(started_at_ts),
            "completed_at_ts": completed_at_ts,
            "completed_at": _fmt_ts(completed_at_ts),
            "queue_wait_ms": queue_wait_ms,
            "queue_wait_sec": _sec(queue_wait_ms),
            "analysis_duration_ms": analysis_duration_ms,
            "analysis_duration_sec": _sec(analysis_duration_ms),
            "total_elapsed_ms": total_elapsed_ms,
            "total_elapsed_sec": _sec(total_elapsed_ms),
        }
        append_event(get_settings().events_log_path, {
            "type": "audit",
            "event": "pdf_analysis",
            "status": "error",
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "duration_ms": analysis_duration_ms,
            "queue_wait_ms": queue_wait_ms,
            "total_elapsed_ms": total_elapsed_ms,
            "accuracy_mode": accuracy_mode,
            "error": str(e),
        })

@router.post("/upload")
async def upload_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    relax_mode: bool = False,
    deterministic: bool = True,
    reuse_existing: bool = True,
    accuracy_mode: Optional[bool] = None,
):
    filename = str(file.filename or "").strip()
    if not filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    try:
        content = await file.read()
        if not content:
             raise HTTPException(status_code=400, detail="Empty file")

        import hashlib
        file_hash = hashlib.sha256(content).hexdigest()
        task_id = str(uuid.uuid4())
        submitted_at_ts = int(time.time())
        
        # 초기 상태 설정
        jobs[task_id] = {
            "status": "pending",
            "message": "대기 중...",
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename or "uploaded.pdf",
            "accuracy_mode": accuracy_mode,
            "file_size_bytes": len(content),
            "submitted_at_ts": submitted_at_ts,
            "submitted_at": _fmt_ts(submitted_at_ts),
        }
        
        # 백그라운드 작업 등록
        background_tasks.add_task(
            process_pdf_task,
            task_id,
            content,
            file_hash,
            filename or "uploaded.pdf",
            relax_mode,
            deterministic,
            reuse_existing,
            accuracy_mode,
        )
        
        return {
            "task_id": task_id,
            "status": "pending",
            "pdf_hash": file_hash,
            "filename": filename or "uploaded.pdf",
            "accuracy_mode": accuracy_mode,
            "submitted_at": _fmt_ts(submitted_at_ts),
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
