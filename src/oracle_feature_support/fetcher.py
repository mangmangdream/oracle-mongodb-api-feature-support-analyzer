from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
import time
from typing import Callable, Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

TARGET_URL = (
    "https://docs.oracle.com/en/database/oracle/mongodb-api/"
    "mgapi/support-mongodb-apis-operations-and-data-types-reference.html"
)
INDEX_URL = "https://docs.oracle.com/en/database/oracle/mongodb-api/mgapi/index.html"
MONTH_YEAR_RE = re.compile(
    r"\b("
    r"January|February|March|April|May|June|July|August|September|October|November|December"
    r")\s+\d{4}\b"
)
DOC_ID_RE = re.compile(r"\bF\d{5}-\d+\b")


@dataclass
class AnalysisResult:
    detail_df: pd.DataFrame
    summary_df: pd.DataFrame
    output_dir: Path
    doc_metadata: dict[str, str]


def _clean_text(text: str) -> str:
    cleaned = (
        text.replace("\u00c2", "")
        .replace("\xa0", " ")
        .replace("\u200b", "")
        .replace("\ufeff", "")
    )
    return " ".join(cleaned.split()).strip()


def _latest_output_dir(output_root: str = "outputs") -> Path | None:
    root = Path(output_root)
    if not root.exists():
        return None
    candidates = [p for p in root.glob("feature_support_*") if p.is_dir()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _load_latest_metadata(output_root: str = "outputs") -> dict[str, str] | None:
    latest_dir = _latest_output_dir(output_root)
    if latest_dir is None:
        return None

    metadata_path = latest_dir / "document_metadata.json"
    if not metadata_path.exists():
        return None

    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _extract_document_metadata(html: str, source_url: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    lines = [
        _clean_text(line)
        for line in soup.get_text("\n", strip=True).splitlines()
        if _clean_text(line)
    ]
    text = "\n".join(lines)

    doc_id_match = DOC_ID_RE.search(text)
    date_match = MONTH_YEAR_RE.search(text)
    title = next((line for line in lines if "Database API for MongoDB" in line), "")

    return {
        "doc_source_url": source_url,
        "doc_title": title,
        "doc_id": doc_id_match.group(0) if doc_id_match else "",
        "doc_version_date": date_match.group(0) if date_match else "",
        "metadata_fetched_at": datetime.now().isoformat(timespec="seconds"),
    }


def _compare_document_metadata(
    current: dict[str, str],
    previous: dict[str, str] | None,
) -> dict[str, str]:
    if previous is None:
        return {
            **current,
            "previous_doc_id": "",
            "previous_doc_version_date": "",
            "update_status": "首次记录或无历史版本信息",
        }

    previous_doc_id = previous.get("doc_id", "")
    previous_doc_version_date = previous.get("doc_version_date", "")
    current_doc_id = current.get("doc_id", "")
    current_doc_version_date = current.get("doc_version_date", "")

    if not current_doc_id and not current_doc_version_date:
        update_status = "无法判断: 未解析到当前文档版本信息"
    elif current_doc_id != previous_doc_id or current_doc_version_date != previous_doc_version_date:
        update_status = "发现文档版本变化，建议重新核对 Feature Support 明细"
    else:
        update_status = "未发现文档版本变化"

    return {
        **current,
        "previous_doc_id": previous_doc_id,
        "previous_doc_version_date": previous_doc_version_date,
        "update_status": update_status,
    }


def fetch_document_metadata(
    index_url: str = INDEX_URL,
    output_root: str = "outputs",
    timeout: int = 30,
    max_retries: int = 3,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, str]:
    if progress_callback:
        progress_callback(f"[DOC] Fetch document metadata -> {index_url}")
    html = fetch_html(
        url=index_url,
        timeout=timeout,
        max_retries=max_retries,
        progress_callback=progress_callback,
    )
    current = _extract_document_metadata(html, index_url)
    previous = _load_latest_metadata(output_root)
    metadata = _compare_document_metadata(current, previous)
    if progress_callback:
        progress_callback(
            "[DOC] "
            f"doc_id={metadata.get('doc_id', '')}, "
            f"doc_version_date={metadata.get('doc_version_date', '')}, "
            f"update_status={metadata.get('update_status', '')}"
        )
    return metadata


def fetch_html(
    url: str = TARGET_URL,
    timeout: int = 30,
    max_retries: int = 3,
    backoff_seconds: float = 1.5,
    progress_callback: Callable[[str], None] | None = None,
) -> str:
    last_error: Exception | None = None
    attempts = max_retries + 1

    for attempt in range(1, attempts + 1):
        try:
            if progress_callback:
                progress_callback(f"[FETCH] Attempt {attempt}/{attempts} -> {url}")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            if progress_callback:
                progress_callback(
                    f"[FETCH] Success status={response.status_code}, bytes={len(response.text)}"
                )
            return response.text
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if progress_callback:
                progress_callback(f"[FETCH] Attempt {attempt} failed: {exc}")
            if attempt == attempts:
                break
            if progress_callback:
                wait_seconds = backoff_seconds * (2 ** (attempt - 1))
                progress_callback(f"[FETCH] Sleep {wait_seconds:.1f}s before retry")
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))

    raise RuntimeError(
        f"Failed to fetch URL after {attempts} attempts (timeout={timeout}s): {last_error}"
    ) from last_error


def _find_anchor_section(soup: BeautifulSoup, anchor_id: str = "index-options") -> list[Tag]:
    anchor = soup.find(id=anchor_id)
    if not anchor:
        return []

    tables: list[Tag] = []
    current: Tag | None = anchor if isinstance(anchor, Tag) else None
    while current is not None:
        current = current.find_next()
        if current is None:
            break
        if current.name in {"h1", "h2"} and current.get("id") != anchor_id:
            break
        if current.name == "table":
            tables.append(current)

    return tables


def _nearest_heading_text(table: Tag) -> str:
    heading = table.find_previous(["h3", "h2", "h1"])
    if not heading:
        return "Unknown Section"
    return _clean_text(heading.get_text(" ", strip=True))


def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed: dict[str, str] = {}
    used_names: dict[str, int] = {}
    for col in df.columns:
        clean_col = _clean_text(str(col))
        low = clean_col.lower()
        if "support" in low and "since" in low:
            target = "Support (Since)"
        elif low == "command":
            target = "Command"
        elif low == "operator":
            target = "Operator"
        elif low == "stage":
            target = "Stage"
        else:
            target = clean_col

        if target in used_names:
            used_names[target] += 1
            target = f"{target}_{used_names[target]}"
        else:
            used_names[target] = 1

        renamed[col] = target

    return df.rename(columns=renamed)


def _table_to_dataframe(table: Tag) -> pd.DataFrame:
    rows: list[list[str]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        rows.append([_clean_text(cell.get_text(" ", strip=True)) for cell in cells])

    if not rows:
        return pd.DataFrame()

    header = rows[0]
    body = rows[1:] if len(rows) > 1 else []

    row_lengths = [len(header), *[len(r) for r in body]]
    max_len = max(row_lengths)
    padded_header = header + [f"column_{i + 1}" for i in range(len(header), max_len)]

    padded_body = []
    for row in body:
        padded_body.append(row + ["" for _ in range(max_len - len(row))])

    df = pd.DataFrame(padded_body, columns=padded_header)
    return _canonicalize_columns(df)


def _extract_feature_support_tables(html: str) -> list[pd.DataFrame]:
    soup = BeautifulSoup(html, "html.parser")

    all_feature_tables: list[pd.DataFrame] = []
    for table in soup.find_all("table"):
        df = _table_to_dataframe(table)
        if df.empty:
            continue
        support_cols = [c for c in df.columns if "support" in c.lower()]
        if not support_cols:
            continue
        section_text = _nearest_heading_text(table)
        temp = df.copy()
        temp.insert(0, "section", section_text)
        all_feature_tables.append(temp)

    return all_feature_tables


def _pick_status_column(columns: Iterable[str]) -> str | None:
    candidates = [c for c in columns if c]
    if "Support (Since)" in candidates:
        return "Support (Since)"
    for col in candidates:
        low = col.lower()
        if "support" in low or "status" in low:
            return col
    return None


def _normalize_status(raw: str) -> str:
    low = raw.lower()
    if any(k in low for k in ["not supported", "unsupported", "no"]):
        return "Not Supported"
    if any(k in low for k in ["n/a", "na", "not applicable"]):
        return "Not Supported"
    if any(k in low for k in ["partial", "limited", "partially"]):
        return "Partially Supported"
    if any(k in low for k in ["supported", "yes", "full"]):
        return "Supported"
    # Versions like 4.2/5.0/6.0 are treated as supported since that version.
    if any(ch.isdigit() for ch in low):
        return "Supported"
    if "always" in low:
        return "Supported"
    return "Other"


def analyze_feature_support(
    url: str = TARGET_URL,
    output_root: str = "outputs",
    timeout: int = 30,
    max_retries: int = 3,
    progress_callback: Callable[[str], None] | None = None,
) -> AnalysisResult:
    if progress_callback:
        progress_callback(
            f"[START] Analyze url={url}, timeout={timeout}s, max_retries={max_retries}"
        )

    try:
        doc_metadata = fetch_document_metadata(
            output_root=output_root,
            timeout=timeout,
            max_retries=max_retries,
            progress_callback=progress_callback,
        )
    except Exception as exc:  # noqa: BLE001
        doc_metadata = {
            "doc_source_url": INDEX_URL,
            "doc_title": "",
            "doc_id": "",
            "doc_version_date": "",
            "metadata_fetched_at": datetime.now().isoformat(timespec="seconds"),
            "previous_doc_id": "",
            "previous_doc_version_date": "",
            "update_status": f"无法判断: 文档版本信息抓取失败 ({exc})",
        }
        if progress_callback:
            progress_callback(f"[DOC] Metadata fetch failed: {exc}")

    html = fetch_html(
        url=url,
        timeout=timeout,
        max_retries=max_retries,
        progress_callback=progress_callback,
    )
    if not doc_metadata.get("doc_id") and not doc_metadata.get("doc_version_date"):
        fallback_metadata = _extract_document_metadata(html, url)
        if fallback_metadata.get("doc_id") or fallback_metadata.get("doc_version_date"):
            doc_metadata = _compare_document_metadata(
                fallback_metadata,
                _load_latest_metadata(output_root),
            )
            if progress_callback:
                progress_callback(
                    "[DOC] Fallback metadata from feature page: "
                    f"doc_id={doc_metadata.get('doc_id', '')}, "
                    f"doc_version_date={doc_metadata.get('doc_version_date', '')}, "
                    f"update_status={doc_metadata.get('update_status', '')}"
                )
    if progress_callback:
        progress_callback("[PARSE] Extracting feature support tables")
    tables = _extract_feature_support_tables(html)
    if progress_callback:
        progress_callback(f"[PARSE] Found {len(tables)} table(s)")

    if not tables:
        raise ValueError("No feature-support table found on the target page.")

    merged = []
    for idx, df in enumerate(tables, start=1):
        temp = df.copy()
        temp.insert(0, "table_index", idx)
        merged.append(temp)

    detail_df = pd.concat(merged, ignore_index=True)
    detail_df = detail_df.replace("", pd.NA).dropna(how="all")
    for col in detail_df.columns:
        if pd.api.types.is_object_dtype(detail_df[col]):
            detail_df[col] = detail_df[col].fillna("").astype(str).map(_clean_text)
    if progress_callback:
        progress_callback(f"[ANALYZE] Detail rows={len(detail_df)}")

    status_col = _pick_status_column(detail_df.columns)
    if status_col is None:
        detail_df["normalized_status"] = "Other"
        if progress_callback:
            progress_callback("[ANALYZE] No status column detected, fallback to Other")
    else:
        detail_df["normalized_status"] = detail_df[status_col].fillna("").astype(str).map(_normalize_status)
        if progress_callback:
            progress_callback(f"[ANALYZE] Status column={status_col}")
            support_non_empty = detail_df[status_col].fillna("").astype(str).str.strip().ne("").sum()
            progress_callback(f"[ANALYZE] Non-empty '{status_col}' rows={support_non_empty}")

    if "Command" in detail_df.columns and progress_callback:
        command_non_empty = detail_df["Command"].fillna("").astype(str).str.strip().ne("").sum()
        progress_callback(f"[ANALYZE] Non-empty 'Command' rows={command_non_empty}")

    status_summary = (
        detail_df["normalized_status"]
        .value_counts(dropna=False)
        .rename_axis("normalized_status")
        .reset_index(name="count")
    )
    status_summary["percentage"] = (status_summary["count"] / status_summary["count"].sum() * 100).round(2)

    summary_parts: list[pd.DataFrame] = [status_summary]
    if status_col is not None:
        support_since_summary = (
            detail_df[status_col]
            .fillna("Unknown")
            .astype(str)
            .value_counts(dropna=False)
            .head(20)
            .rename_axis("support_value")
            .reset_index(name="count")
        )
        support_since_summary["metric"] = "support_value_top20"
        support_since_summary["percentage"] = (
            support_since_summary["count"] / support_since_summary["count"].sum() * 100
        ).round(2)
        summary_parts.append(support_since_summary.rename(columns={"support_value": "normalized_status"}))

    section_summary = (
        detail_df["section"]
        .fillna("Unknown Section")
        .astype(str)
        .value_counts(dropna=False)
        .rename_axis("normalized_status")
        .reset_index(name="count")
    )
    section_summary["metric"] = "section_count"
    section_summary["percentage"] = (section_summary["count"] / section_summary["count"].sum() * 100).round(2)
    summary_parts.append(section_summary)

    status_summary["metric"] = "normalized_status"
    summary = pd.concat(summary_parts, ignore_index=True, sort=False)
    if "metric" in summary.columns:
        summary = summary[["metric", "normalized_status", "count", "percentage"]]
    else:
        summary = summary[["normalized_status", "count", "percentage"]]
    if progress_callback:
        progress_callback(f"[ANALYZE] Summary categories={len(summary)}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(output_root) / f"feature_support_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    detail_path = output_dir / "feature_support_detail.csv"
    summary_path = output_dir / "feature_support_summary.csv"
    metadata_path = output_dir / "document_metadata.json"

    detail_df.to_csv(detail_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    metadata_path.write_text(
        json.dumps(doc_metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if progress_callback:
        progress_callback(f"[SAVE] Detail -> {detail_path}")
        progress_callback(f"[SAVE] Summary -> {summary_path}")
        progress_callback(f"[SAVE] Metadata -> {metadata_path}")

    report_path = output_dir / "report.md"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Feature Support Analysis\n\n")
        f.write(f"- Source URL: {url}\n")
        f.write(f"- Document version date: {doc_metadata.get('doc_version_date', '')}\n")
        f.write(f"- Document ID: {doc_metadata.get('doc_id', '')}\n")
        f.write(f"- Update status: {doc_metadata.get('update_status', '')}\n")
        f.write(f"- Total records: {len(detail_df)}\n")
        f.write("\n## Summary\n\n")
        f.write(summary.to_markdown(index=False))
        f.write("\n")
    if progress_callback:
        progress_callback(f"[SAVE] Report -> {report_path}")
        progress_callback("[DONE] Analysis completed")

    return AnalysisResult(
        detail_df=detail_df,
        summary_df=summary,
        output_dir=output_dir,
        doc_metadata=doc_metadata,
    )
