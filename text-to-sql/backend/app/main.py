from fastapi import FastAPI

from app.api.routes import admin_budget, admin_metadata, admin_oracle, query, report

app = FastAPI(title="RAG SQL Demo API", version="0.1.0")

app.include_router(admin_metadata.router, prefix="/admin/metadata", tags=["admin-metadata"])
app.include_router(admin_metadata.rag_router, prefix="/admin/rag", tags=["admin-rag"])
app.include_router(query.router, prefix="/query", tags=["query"])
app.include_router(report.router, prefix="/report", tags=["report"])
app.include_router(admin_budget.router, prefix="/admin/budget", tags=["admin-budget"])
app.include_router(admin_oracle.router, prefix="/admin/oracle", tags=["admin-oracle"])
