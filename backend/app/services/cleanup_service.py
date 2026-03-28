from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from backend.app.models import CleanupArtifacts, CleanupMetrics


EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)

COUNTRY_MAP = {
    "u.s.a.": "United States",
    "usa": "United States",
    "us": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
}

STATUS_MAP = {
    "active ": "Active",
    "active": "Active",
    "pending": "Pending",
    "in progress": "In Progress",
    "in-progress": "In Progress",
}


def _normalize_string(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_email(value: object) -> str:
    email = _normalize_string(value).lower()
    return email if EMAIL_RE.match(email) else email


def _normalize_phone(value: object) -> str:
    raw = re.sub(r"\D", "", _normalize_string(value))
    if not raw:
        return ""
    if len(raw) == 10:
        return f"+1-{raw[:3]}-{raw[3:6]}-{raw[6:]}"
    if len(raw) == 11 and raw.startswith("1"):
        return f"+1-{raw[1:4]}-{raw[4:7]}-{raw[7:]}"
    return raw


def _standardize_category(value: object, mapping: dict[str, str]) -> str:
    raw = _normalize_string(value)
    if not raw:
        return ""
    return mapping.get(raw.lower(), raw.title())


def _detect_semantic_columns(columns: list[str]) -> dict[str, str]:
    semantic: dict[str, str] = {}
    for column in columns:
        lowered = column.lower()
        if "email" in lowered:
            semantic[column] = "email"
        elif "phone" in lowered or "mobile" in lowered:
            semantic[column] = "phone"
        elif "date" in lowered or "signup" in lowered:
            semantic[column] = "date"
        elif "country" in lowered:
            semantic[column] = "country"
        elif "status" in lowered or "stage" in lowered:
            semantic[column] = "status"
        elif any(token in lowered for token in ("name", "city", "state", "company")):
            semantic[column] = "title"
    return semantic


def process_csv(input_path: Path, output_dir: Path, job_id: str) -> CleanupArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path)
    rows_before = len(df)
    original_df = df.copy(deep=True)

    for column in df.columns:
        if df[column].dtype == "object":
            df[column] = df[column].apply(_normalize_string)

    whitespace_trimmed = int((original_df.astype(str) != df.astype(str)).sum().sum())

    semantic_columns = _detect_semantic_columns(df.columns.tolist())
    emails_normalized = 0
    phones_normalized = 0
    dates_normalized = 0
    categorical_standardized = 0

    for column, kind in semantic_columns.items():
        before = df[column].copy()
        if kind == "email":
            df[column] = df[column].apply(_normalize_email)
            emails_normalized += int((before != df[column]).sum())
        elif kind == "phone":
            df[column] = df[column].apply(_normalize_phone)
            phones_normalized += int((before != df[column]).sum())
        elif kind == "date":
            parsed = pd.to_datetime(df[column], errors="coerce", dayfirst=False)
            normalized = parsed.dt.strftime("%Y-%m-%d").fillna("")
            dates_normalized += int((before != normalized).sum())
            df[column] = normalized
        elif kind == "country":
            df[column] = df[column].apply(lambda value: _standardize_category(value, COUNTRY_MAP))
            categorical_standardized += int((before != df[column]).sum())
        elif kind == "status":
            df[column] = df[column].apply(lambda value: _standardize_category(value, STATUS_MAP))
            categorical_standardized += int((before != df[column]).sum())
        elif kind == "title":
            df[column] = df[column].apply(lambda value: _normalize_string(value).title())
            categorical_standardized += int((before != df[column]).sum())

    missing_values_by_column = {
        column: int(df[column].replace("", pd.NA).isna().sum()) for column in df.columns
    }

    duplicates_removed = int(df.duplicated().sum())
    df = df.drop_duplicates().reset_index(drop=True)
    rows_after = len(df)

    notes = []
    if duplicates_removed:
        notes.append(f"Removed {duplicates_removed} duplicate rows.")
    if emails_normalized:
        notes.append(f"Normalized {emails_normalized} email values.")
    if phones_normalized:
        notes.append(f"Normalized {phones_normalized} phone values.")
    if dates_normalized:
        notes.append(f"Standardized {dates_normalized} date values.")
    if categorical_standardized:
        notes.append(f"Standardized {categorical_standardized} categorical values.")
    if not notes:
        notes.append("Applied baseline cleanup rules and generated an audit summary.")

    cleaned_csv_path = output_dir / "cleaned.csv"
    report_json_path = output_dir / "report.json"
    report_html_path = output_dir / "report.html"

    df.to_csv(cleaned_csv_path, index=False)

    metrics = CleanupMetrics(
        rows_before=rows_before,
        rows_after=rows_after,
        duplicates_removed=duplicates_removed,
        whitespace_trimmed=whitespace_trimmed,
        emails_normalized=emails_normalized,
        phones_normalized=phones_normalized,
        dates_normalized=dates_normalized,
        categorical_standardized=categorical_standardized,
        missing_values_by_column=missing_values_by_column,
        notes=notes,
    )

    report_payload = {
        "job_id": job_id,
        "source_file": input_path.name,
        "metrics": metrics.to_dict(),
        "columns": df.columns.tolist(),
        "preview": df.head(10).fillna("").to_dict(orient="records"),
    }

    report_json_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
    report_html_path.write_text(_build_html_report(report_payload), encoding="utf-8")

    preview = df.head(10).fillna("").astype(str).to_dict(orient="records")

    return CleanupArtifacts(
        job_id=job_id,
        original_filename=input_path.name,
        cleaned_csv_path=cleaned_csv_path,
        report_json_path=report_json_path,
        report_html_path=report_html_path,
        cleaned_preview=preview,
        metrics=metrics,
        execution_mode="local",
    )


def _build_html_report(report_payload: dict) -> str:
    metrics = report_payload["metrics"]
    preview_rows = report_payload["preview"]
    columns = report_payload["columns"]
    header_cells = "".join(f"<th>{column}</th>" for column in columns)
    body_rows = "".join(
        "<tr>" + "".join(f"<td>{row.get(column, '')}</td>" for column in columns) + "</tr>"
        for row in preview_rows
    )
    missing_items = "".join(
        f"<li><strong>{column}</strong>: {count}</li>"
        for column, count in metrics["missing_values_by_column"].items()
    )
    notes = "".join(f"<li>{note}</li>" for note in metrics["notes"])
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CleanSlate Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 2rem; background: #f7f4ee; color: #1f2937; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 2rem; }}
    .card {{ background: white; border-radius: 14px; padding: 1rem; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
    h1, h2 {{ margin-bottom: 0.5rem; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 12px; overflow: hidden; }}
    th, td {{ padding: 0.75rem; border-bottom: 1px solid #e5e7eb; text-align: left; font-size: 0.92rem; }}
    th {{ background: #0f766e; color: white; }}
    ul {{ padding-left: 1.2rem; }}
  </style>
</head>
<body>
  <h1>CleanSlate Report</h1>
  <p>Job ID: <strong>{report_payload["job_id"]}</strong></p>
  <p>Source file: <strong>{report_payload["source_file"]}</strong></p>
  <div class="grid">
    <div class="card"><h2>Rows Before</h2><p>{metrics["rows_before"]}</p></div>
    <div class="card"><h2>Rows After</h2><p>{metrics["rows_after"]}</p></div>
    <div class="card"><h2>Duplicates Removed</h2><p>{metrics["duplicates_removed"]}</p></div>
    <div class="card"><h2>Emails Normalized</h2><p>{metrics["emails_normalized"]}</p></div>
    <div class="card"><h2>Phones Normalized</h2><p>{metrics["phones_normalized"]}</p></div>
    <div class="card"><h2>Dates Standardized</h2><p>{metrics["dates_normalized"]}</p></div>
  </div>
  <div class="card">
    <h2>Audit Notes</h2>
    <ul>{notes}</ul>
  </div>
  <div class="card">
    <h2>Missing Values</h2>
    <ul>{missing_items}</ul>
  </div>
  <h2>Preview</h2>
  <table>
    <thead><tr>{header_cells}</tr></thead>
    <tbody>{body_rows}</tbody>
  </table>
</body>
</html>"""

