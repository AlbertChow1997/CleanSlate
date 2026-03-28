from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path


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
        return {
            "job_id": self.job_id,
            "original_filename": self.original_filename,
            "execution_mode": self.execution_mode,
            "cleaned_csv_url": f"/downloads/{self.job_id}/cleaned.csv",
            "report_json_url": f"/downloads/{self.job_id}/report.json",
            "report_html_url": f"/reports/{self.job_id}",
            "cleaned_preview": self.cleaned_preview,
            "metrics": self.metrics.to_dict(),
        }

