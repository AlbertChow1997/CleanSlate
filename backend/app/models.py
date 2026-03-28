from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import shutil


@dataclass
class CleanupMetrics:
    rows_before: int
    rows_after: int
    duplicates_removed: int
    whitespace_trimmed: int
    emails_normalized: int
    phones_normalized: int
    dates_normalized: int
    categorical_standardized: int
    missing_values_by_column: dict[str, int]
    notes: list[str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CleanupArtifacts:
    job_id: str
    original_filename: str
    cleaned_csv_path: Path
    report_json_path: Path
    report_html_path: Path
    cleaned_preview: list[dict[str, str]]
    metrics: CleanupMetrics
    execution_mode: str

    def to_response(self) -> dict:
        cleaned_csv_filename = f"{Path(self.original_filename).stem or self.job_id}-cleaned.csv"
        report_json_filename = f"{Path(self.original_filename).stem or self.job_id}-report.json"
        report_html_filename = f"{Path(self.original_filename).stem or self.job_id}-report.html"
        return {
            "job_id": self.job_id,
            "original_filename": self.original_filename,
            "execution_mode": self.execution_mode,
            "cleaned_csv_filename": cleaned_csv_filename,
            "cleaned_csv_content": self.cleaned_csv_path.read_text(encoding="utf-8"),
            "report_json_filename": report_json_filename,
            "report_json_content": self.report_json_path.read_text(encoding="utf-8"),
            "report_html_filename": report_html_filename,
            "report_html_content": self.report_html_path.read_text(encoding="utf-8"),
            "cleaned_preview": self.cleaned_preview,
            "metrics": self.metrics.to_dict(),
        }

    def cleanup(self) -> None:
        workspace_dir = self.cleaned_csv_path.parent.parent
        if workspace_dir.exists():
            shutil.rmtree(workspace_dir, ignore_errors=True)
