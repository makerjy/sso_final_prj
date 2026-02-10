from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from app.api.routes import (
    admin_budget,
    admin_metadata,
    admin_oracle,
    admin_settings,
    audit,
    chat,
    cohort,
    dashboard,
    query,
    report,
)

app = FastAPI(title="RAG SQL Demo API", version="0.1.0")

origins = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000",
    ).split(",")
    if origin.strip()
]
if origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(admin_metadata.router, prefix="/admin/metadata", tags=["admin-metadata"])
app.include_router(admin_metadata.rag_router, prefix="/admin/rag", tags=["admin-rag"])
app.include_router(admin_settings.router, prefix="/admin/settings", tags=["admin-settings"])
app.include_router(audit.router, prefix="/audit", tags=["audit"])
app.include_router(chat.router, prefix="/chat", tags=["chat"])
app.include_router(cohort.router, prefix="/cohort", tags=["cohort"])
app.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
app.include_router(query.router, prefix="/query", tags=["query"])
app.include_router(report.router, prefix="/report", tags=["report"])
app.include_router(admin_budget.router, prefix="/admin/budget", tags=["admin-budget"])
app.include_router(admin_oracle.router, prefix="/admin/oracle", tags=["admin-oracle"])
