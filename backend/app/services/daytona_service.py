from __future__ import annotations

import csv
import json
import shutil
import tempfile
import uuid
from pathlib import Path

from backend.app.config import settings
from backend.app.models import CleanupArtifacts, CleanupMetrics
from backend.app.services.cleanup_service import process_csv
from backend.app.services.daytona_remote_worker import REMOTE_WORKER_SCRIPT


class DaytonaExecutor:
    def __init__(self) -> None:
        self._sdk_available = False
        self._sdk_error: str | None = None
        self._daytona_cls = None
        self._config_cls = None
        try:
            from daytona import Daytona, DaytonaConfig

            self._daytona_cls = Daytona
            self._config_cls = DaytonaConfig
            self._sdk_available = True
        except Exception as exc:  # pragma: no cover
            self._sdk_error = str(exc)

    def execute_cleanup(self, source_file: Path) -> CleanupArtifacts:
        job_id = uuid.uuid4().hex[:10]
        job_root_dir = Path(tempfile.mkdtemp(prefix=f"cleanslate-{job_id}-"))
        job_upload_dir = job_root_dir / "input"
        job_result_dir = job_root_dir / "output"
        job_upload_dir.mkdir(parents=True, exist_ok=True)
        job_result_dir.mkdir(parents=True, exist_ok=True)

        working_file = job_upload_dir / source_file.name
        shutil.copy2(source_file, working_file)

        if settings.use_daytona and self._sdk_available and settings.daytona_api_key:
            try:
                return self._execute_cleanup_in_daytona(working_file, job_result_dir, job_id)
            except Exception as exc:
                artifacts = process_csv(working_file, job_result_dir, job_id)
                artifacts.execution_mode = f"local-fallback ({type(exc).__name__})"
                return artifacts

        artifacts = process_csv(working_file, job_result_dir, job_id)
        artifacts.execution_mode = "local-fallback"
        return artifacts

    def _execute_cleanup_in_daytona(
        self,
        source_file: Path,
        job_result_dir: Path,
        job_id: str,
    ) -> CleanupArtifacts:
        config_kwargs = {"api_key": settings.daytona_api_key}
        if settings.daytona_target:
            config_kwargs["target"] = settings.daytona_target
        if settings.daytona_api_url:
            config_kwargs["api_url"] = settings.daytona_api_url
        if settings.daytona_server_url:
            config_kwargs["server_url"] = settings.daytona_server_url

        config = self._config_cls(**config_kwargs)
        client = self._daytona_cls(config)
        sandbox = None

        try:
            sandbox = client.create(timeout=120)

            remote_root = f"{sandbox.get_work_dir().rstrip('/')}/cleanslate"
            remote_input = f"{remote_root}/input/{source_file.name}"
            remote_output = f"{remote_root}/output"
            remote_worker = f"{remote_root}/worker.py"

            sandbox.fs.create_folder(remote_root, "755")
            sandbox.fs.create_folder(f"{remote_root}/input", "755")
            sandbox.fs.create_folder(remote_output, "755")

            sandbox.fs.upload_file(str(source_file), remote_input)
            sandbox.fs.upload_file(REMOTE_WORKER_SCRIPT.encode("utf-8"), remote_worker)

            response = sandbox.process.exec(
                f"python {remote_worker} --input {remote_input} --output-dir {remote_output} --job-id {job_id}",
                cwd=remote_root,
                timeout=120,
            )
            if response.exit_code not in (0, None):
                raise RuntimeError(response.result or "Daytona cleanup script failed.")

            cleaned_csv_path = job_result_dir / "cleaned.csv"
            report_json_path = job_result_dir / "report.json"
            report_html_path = job_result_dir / "report.html"

            sandbox.fs.download_file(f"{remote_output}/cleaned.csv", str(cleaned_csv_path))
            sandbox.fs.download_file(f"{remote_output}/report.json", str(report_json_path))
            sandbox.fs.download_file(f"{remote_output}/report.html", str(report_html_path))

            return self._build_artifacts_from_downloads(
                source_file=source_file,
                job_id=job_id,
                cleaned_csv_path=cleaned_csv_path,
                report_json_path=report_json_path,
                report_html_path=report_html_path,
            )
        finally:
            if sandbox is not None:
                sandbox.delete()

    def _build_artifacts_from_downloads(
        self,
        source_file: Path,
        job_id: str,
        cleaned_csv_path: Path,
        report_json_path: Path,
        report_html_path: Path,
    ) -> CleanupArtifacts:
        payload = json.loads(report_json_path.read_text(encoding="utf-8"))
        with cleaned_csv_path.open("r", newline="", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            preview = []
            for index, row in enumerate(reader):
                if index >= 10:
                    break
                preview.append({key: value or "" for key, value in row.items()})

        metrics = CleanupMetrics(**payload["metrics"])
        return CleanupArtifacts(
            job_id=job_id,
            original_filename=source_file.name,
            cleaned_csv_path=cleaned_csv_path,
            report_json_path=report_json_path,
            report_html_path=report_html_path,
            cleaned_preview=preview,
            metrics=metrics,
            execution_mode="daytona",
        )


executor = DaytonaExecutor()
