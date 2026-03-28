from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from fastapi import APIRouter, File, HTTPException, UploadFile

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
        try:
            return artifacts.to_response()
        finally:
            artifacts.cleanup()
    finally:
        if tmp_path and tmp_path.exists():
            os.unlink(tmp_path)
