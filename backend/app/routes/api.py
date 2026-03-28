from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse

from backend.app.config import RESULTS_DIR
from backend.app.services.daytona_service import executor

router = APIRouter(prefix="/api", tags=["api"])


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/process")
async def process_file(file: UploadFile = File(...)) -> dict:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a CSV file.")

    tmp_path: Path | None = None
    try:
        with NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            contents = await file.read()
            tmp.write(contents)
            tmp_path = Path(tmp.name)

        artifacts = executor.execute_cleanup(tmp_path)
        artifacts.original_filename = file.filename
        return artifacts.to_response()
    finally:
        if tmp_path and tmp_path.exists():
            os.unlink(tmp_path)


@router.get("/reports/{job_id}", response_class=HTMLResponse)
async def get_report(job_id: str) -> HTMLResponse:
    report_path = RESULTS_DIR / job_id / "report.html"
    if not report_path.exists():
        raise HTTPException(status_code=404, detail="Report not found.")
    return HTMLResponse(report_path.read_text(encoding="utf-8"))


@router.get("/downloads/{job_id}/cleaned.csv")
async def download_cleaned_csv(job_id: str) -> FileResponse:
    cleaned_path = RESULTS_DIR / job_id / "cleaned.csv"
    if not cleaned_path.exists():
        raise HTTPException(status_code=404, detail="Cleaned CSV not found.")
    return FileResponse(cleaned_path, filename=f"{job_id}-cleaned.csv", media_type="text/csv")


@router.get("/downloads/{job_id}/report.json")
async def download_report_json(job_id: str) -> FileResponse:
    report_path = RESULTS_DIR / job_id / "report.json"
    if not report_path.exists():
        raise HTTPException(status_code=404, detail="Report JSON not found.")
    return FileResponse(report_path, filename=f"{job_id}-report.json", media_type="application/json")
