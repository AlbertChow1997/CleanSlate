REMOTE_WORKER_SCRIPT = r'''
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path


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

DATE_FORMATS = [
    "%m/%d/%y",
    "%Y-%m-%d",
    "%B %d %Y",
    "%Y/%m/%d",
    "%m-%d-%Y",
    "%Y-%m-%d %H:%M:%S",
]


def normalize_string(value: str) -> str:
    return (value or "").strip()


def normalize_email(value: str) -> str:
    email = normalize_string(value).lower()
    return email if EMAIL_RE.match(email) else email


def normalize_phone(value: str) -> str:
    digits = re.sub(r"\D", "", normalize_string(value))
    if not digits:
        return ""
    if len(digits) == 10:
        return f"+1-{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+1-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    return digits


def normalize_date(value: str) -> str:
    raw = normalize_string(value)
    if not raw:
        return ""
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw


def standardize_category(value: str, mapping: dict[str, str]) -> str:
    raw = normalize_string(value)
    if not raw:
        return ""
    return mapping.get(raw.lower(), raw.title())


def detect_semantic_columns(columns: list[str]) -> dict[str, str]:
    semantic = {}
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


def build_html_report(payload: dict) -> str:
    metrics = payload["metrics"]
    preview_rows = payload["preview"]
    columns = payload["columns"]
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
  <p>Job ID: <strong>{payload["job_id"]}</strong></p>
  <p>Source file: <strong>{payload["source_file"]}</strong></p>
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
        columns = reader.fieldnames or []

    rows_before = len(rows)
    semantic_columns = detect_semantic_columns(columns)

    whitespace_trimmed = 0
    emails_normalized = 0
    phones_normalized = 0
    dates_normalized = 0
    categorical_standardized = 0

    normalized_rows = []
    for row in rows:
        clean_row = {}
        for column in columns:
            value = row.get(column, "")
            trimmed = normalize_string(value)
            if trimmed != (value or ""):
                whitespace_trimmed += 1

            kind = semantic_columns.get(column)
            final_value = trimmed
            if kind == "email":
                final_value = normalize_email(trimmed)
                if final_value != trimmed:
                    emails_normalized += 1
            elif kind == "phone":
                final_value = normalize_phone(trimmed)
                if final_value != trimmed:
                    phones_normalized += 1
            elif kind == "date":
                final_value = normalize_date(trimmed)
                if final_value != trimmed:
                    dates_normalized += 1
            elif kind == "country":
                final_value = standardize_category(trimmed, COUNTRY_MAP)
                if final_value != trimmed:
                    categorical_standardized += 1
            elif kind == "status":
                final_value = standardize_category(trimmed, STATUS_MAP)
                if final_value != trimmed:
                    categorical_standardized += 1
            elif kind == "title":
                final_value = trimmed.title()
                if final_value != trimmed:
                    categorical_standardized += 1

            clean_row[column] = final_value
        normalized_rows.append(clean_row)

    deduped_rows = []
    seen = set()
    for row in normalized_rows:
        key = tuple((column, row.get(column, "")) for column in columns)
        if key not in seen:
            seen.add(key)
            deduped_rows.append(row)

    duplicates_removed = len(normalized_rows) - len(deduped_rows)
    rows_after = len(deduped_rows)

    missing_values_by_column = {
        column: sum(1 for row in deduped_rows if not normalize_string(row.get(column, "")))
        for column in columns
    }

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

    metrics = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "duplicates_removed": duplicates_removed,
        "whitespace_trimmed": whitespace_trimmed,
        "emails_normalized": emails_normalized,
        "phones_normalized": phones_normalized,
        "dates_normalized": dates_normalized,
        "categorical_standardized": categorical_standardized,
        "missing_values_by_column": missing_values_by_column,
        "notes": notes,
    }

    cleaned_csv_path = output_dir / "cleaned.csv"
    report_json_path = output_dir / "report.json"
    report_html_path = output_dir / "report.html"

    with cleaned_csv_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=columns)
        writer.writeheader()
        writer.writerows(deduped_rows)

    payload = {
        "job_id": args.job_id,
        "source_file": input_path.name,
        "metrics": metrics,
        "columns": columns,
        "preview": deduped_rows[:10],
    }

    report_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    report_html_path.write_text(build_html_report(payload), encoding="utf-8")


if __name__ == "__main__":
    main()
'''
