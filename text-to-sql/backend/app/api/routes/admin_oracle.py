from fastapi import APIRouter

from app.services.oracle.connection import pool_status

router = APIRouter()


@router.get("/pool/status")
def oracle_pool_status():
    return pool_status()
