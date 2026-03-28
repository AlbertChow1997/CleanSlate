from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.app.config import FRONTEND_DIR, STATIC_DIR, settings
from backend.app.routes.api import router as api_router

app = FastAPI(title=settings.app_name)
app.include_router(api_router)
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")


@app.get("/")
async def serve_index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/app.js")
async def serve_js() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "app.js")


@app.get("/styles.css")
async def serve_css() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "styles.css")

