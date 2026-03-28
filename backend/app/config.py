from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]
APP_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "backend" / "uploads"
RESULTS_DIR = BASE_DIR / "backend" / "results"
STATIC_DIR = APP_DIR / "static"
TEMPLATES_DIR = APP_DIR / "templates"
FRONTEND_DIR = BASE_DIR / "frontend"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


class Settings:
    app_name: str = "CleanSlate"
    use_daytona: bool = os.getenv("USE_DAYTONA", "false").lower() == "true"
    daytona_api_key: str | None = os.getenv("DAYTONA_API_KEY")
    daytona_target: str | None = os.getenv("DAYTONA_TARGET")
    daytona_api_url: str | None = os.getenv("DAYTONA_API_URL")
    daytona_server_url: str | None = os.getenv("DAYTONA_SERVER_URL")


settings = Settings()
