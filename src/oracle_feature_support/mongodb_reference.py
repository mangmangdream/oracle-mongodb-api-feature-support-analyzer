from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import time
from typing import Callable
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag


CATALOG_PATH = Path("outputs/mongodb_reference_catalog.csv")
METADATA_PATH = Path("outputs/mongodb_reference_metadata.json")
ABOUT_URL = "https://www.mongodb.com/docs/manual/about/"

MONGODB_REFERENCE_SOURCES = [
    {
        "label": "Database Commands",
        "url": "https://www.mongodb.com/docs/manual/reference/command/",
        "entity_type": "command",
        "source_group": "command",
    },
    {
        "label": "Aggregation Stages",
        "url": "https://www.mongodb.com/docs/manual/reference/mql/aggregation-stages/",
        "entity_type": "stage",
        "source_group": "aggregation_stage",
    },
    {
        "label": "Query Predicates",
        "url": "https://www.mongodb.com/docs/manual/reference/mql/query-predicates/",
        "entity_type": "operator",
        "source_group": "query_operator",
    },
    {
        "label": "Update Operators",
        "url": "https://www.mongodb.com/docs/manual/reference/mql/update/",
        "entity_type": "operator",
        "source_group": "update_operator",
    },
    {
        "label": "Projection Operators",
        "url": "https://www.mongodb.com/docs/manual/reference/operator/projection/",
        "entity_type": "operator",
        "source_group": "projection_operator",
    },
    {
        "label": "Expressions",
        "url": "https://www.mongodb.com/docs/manual/reference/mql/expressions/",
        "entity_type": "operator",
        "source_group": "expression_operator",
    },
]


@dataclass
class ReferenceSyncResult:
    reference_df: pd.DataFrame
    metadata: dict[str, str | int]
    catalog_path: Path
    metadata_path: Path


def _clean_text(text: str) -> str:
    return " ".join(
        text.replace("\u00c2", "")
        .replace("\xa0", " ")
        .replace("\u200b", "")
        .replace("\ufeff", "")
        .split()
    ).strip()


def _cell_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return _clean_text(str(value))


def _fetch_html(
    url: str,
    timeout: int = 30,
    max_retries: int = 3,
    progress_callback: Callable[[str], None] | None = None,
) -> str:
    last_error: Exception | None = None
    attempts = max_retries + 1
    for attempt in range(1, attempts + 1):
        try:
            if progress_callback:
                progress_callback(f"[REF] Attempt {attempt}/{attempts} -> {url}")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if progress_callback:
                progress_callback(f"[REF] Attempt {attempt} failed: {exc}")
            if attempt == attempts:
                break
            time.sleep(1.2 * (2 ** (attempt - 1)))

    raise RuntimeError(
        f"Failed to fetch MongoDB reference after {attempts} attempts: {last_error}"
    ) from last_error


def _nearest_heading_text(table: Tag) -> str:
    heading = table.find_previous(["h4", "h3", "h2", "h1"])
    if not heading:
        return ""
    return _clean_text(heading.get_text(" ", strip=True))


def _hash_row(name: str, source_group: str, description: str, doc_url: str) -> str:
    value = "|".join([name, source_group, description, doc_url])
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _table_headers(table: Tag) -> list[str]:
    return [_clean_text(th.get_text(" ", strip=True)) for th in table.find_all("th")]


def _extract_rows_from_source(
    html: str,
    source: dict[str, str],
    progress_callback: Callable[[str], None] | None = None,
) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []

    for table in soup.find_all("table"):
        headers = _table_headers(table)
        if "Description" not in headers:
            continue
        if not any(header in headers for header in ["Name", "Stage", "Command"]):
            continue

        first_header = next(
            (header for header in ["Name", "Stage", "Command"] if header in headers),
            "",
        )
        description_idx = headers.index("Description")
        first_idx = headers.index(first_header)
        category = _nearest_heading_text(table) or source["label"]

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) <= max(first_idx, description_idx):
                continue

            name_cell = cells[first_idx]
            desc_cell = cells[description_idx]
            link = name_cell.find("a")
            name = _clean_text(name_cell.get_text(" ", strip=True))
            description = _clean_text(desc_cell.get_text(" ", strip=True))
            if not name or not description:
                continue

            doc_url = urljoin(source["url"], link.get("href", "")) if link else source["url"]
            rows.append(
                {
                    "entity_type": source["entity_type"],
                    "source_group": source["source_group"],
                    "category": category,
                    "name": name,
                    "short_description": description,
                    "doc_url": doc_url,
                    "source_page_url": source["url"],
                    "source_label": source["label"],
                }
            )

    if progress_callback:
        progress_callback(
            f"[REF] Extracted {len(rows)} entries from {source['label']}"
        )
    return rows


def _load_existing_catalog() -> pd.DataFrame:
    if not CATALOG_PATH.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(CATALOG_PATH, encoding="utf-8-sig")
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


def _extract_manual_version(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    text = " ".join(soup.get_text(" ", strip=True).split())
    match = re.search(
        r"reflects version\s+(\d+\.\d+)\s+of MongoDB",
        text,
        re.IGNORECASE,
    )
    return match.group(1) if match else ""


def sync_mongodb_reference_catalog(
    timeout: int = 30,
    max_retries: int = 3,
    progress_callback: Callable[[str], None] | None = None,
) -> ReferenceSyncResult:
    if progress_callback:
        progress_callback(f"[REF] Fetch manual version -> {ABOUT_URL}")
    about_html = _fetch_html(
        ABOUT_URL,
        timeout=timeout,
        max_retries=max_retries,
        progress_callback=progress_callback,
    )
    manual_version = _extract_manual_version(about_html)
    if progress_callback:
        progress_callback(
            f"[REF] MongoDB manual version={manual_version or 'unknown'}"
        )

    all_rows: list[dict[str, str]] = []
    for source in MONGODB_REFERENCE_SOURCES:
        if progress_callback:
            progress_callback(f"[REF] Sync source -> {source['label']}")
        html = _fetch_html(
            source["url"],
            timeout=timeout,
            max_retries=max_retries,
            progress_callback=progress_callback,
        )
        all_rows.extend(_extract_rows_from_source(html, source, progress_callback))

    if not all_rows:
        raise ValueError("No MongoDB reference entries were extracted from official docs.")

    reference_df = pd.DataFrame(all_rows).drop_duplicates(
        subset=["entity_type", "source_group", "name"],
        keep="first",
    )
    reference_df["last_synced_at"] = datetime.now().isoformat(timespec="seconds")
    reference_df["content_hash"] = reference_df.apply(
        lambda row: _hash_row(
            str(row["name"]),
            str(row["source_group"]),
            str(row["short_description"]),
            str(row["doc_url"]),
        ),
        axis=1,
    )
    reference_df = reference_df.sort_values(
        by=["source_group", "category", "name"],
        kind="stable",
    ).reset_index(drop=True)

    previous_df = _load_existing_catalog()
    previous_hash_map = {}
    if not previous_df.empty and "content_hash" in previous_df.columns:
        previous_hash_map = {
            (str(row["entity_type"]), str(row["source_group"]), str(row["name"])): str(row["content_hash"])
            for _, row in previous_df.iterrows()
        }

    current_hash_map = {
        (str(row["entity_type"]), str(row["source_group"]), str(row["name"])): str(row["content_hash"])
        for _, row in reference_df.iterrows()
    }
    new_count = sum(1 for key in current_hash_map if key not in previous_hash_map)
    updated_count = sum(
        1
        for key, value in current_hash_map.items()
        if key in previous_hash_map and previous_hash_map[key] != value
    )

    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    reference_df.to_csv(CATALOG_PATH, index=False, encoding="utf-8-sig")

    metadata: dict[str, str | int] = {
        "synced_at": datetime.now().isoformat(timespec="seconds"),
        "entry_count": int(len(reference_df)),
        "new_entry_count": int(new_count),
        "updated_entry_count": int(updated_count),
        "source_count": int(len(MONGODB_REFERENCE_SOURCES)),
        "mongodb_manual_version": manual_version,
        "mongodb_manual_about_url": ABOUT_URL,
    }
    METADATA_PATH.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if progress_callback:
        progress_callback(f"[REF] Catalog -> {CATALOG_PATH}")
        progress_callback(f"[REF] Metadata -> {METADATA_PATH}")
        progress_callback(
            "[REF] Sync complete: "
            f"entries={len(reference_df)}, new={new_count}, updated={updated_count}"
        )

    return ReferenceSyncResult(
        reference_df=reference_df,
        metadata=metadata,
        catalog_path=CATALOG_PATH,
        metadata_path=METADATA_PATH,
    )


def load_mongodb_reference_catalog() -> pd.DataFrame:
    return _load_existing_catalog()


def load_mongodb_reference_metadata() -> dict[str, str | int]:
    if not METADATA_PATH.exists():
        return {}
    try:
        return json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def infer_reference_key(row: pd.Series) -> tuple[str, str, str]:
    section = _cell_text(row.get("section", "")).lower()
    command = _cell_text(row.get("Command", ""))
    stage = _cell_text(row.get("Stage", ""))
    operator = _cell_text(row.get("Operator", ""))

    if command:
        return ("command", "command", command)
    if stage:
        return ("stage", "aggregation_stage", stage)
    if operator:
        if "update" in section:
            return ("operator", "update_operator", operator)
        if "projection" in section:
            return ("operator", "projection_operator", operator)
        if "query" in section:
            return ("operator", "query_operator", operator)
        return ("operator", "expression_operator", operator)

    return ("", "", "")


def enrich_feature_support_detail(
    detail_df: pd.DataFrame,
    reference_df: pd.DataFrame,
) -> pd.DataFrame:
    if detail_df.empty or reference_df.empty:
        return detail_df.copy()

    enriched = detail_df.copy()
    inferred = enriched.apply(infer_reference_key, axis=1, result_type="expand")
    inferred.columns = ["mongo_entity_type", "mongo_source_group", "mongo_name"]
    enriched = pd.concat([enriched, inferred], axis=1)

    exact_index = {
        (str(row["entity_type"]), str(row["source_group"]), str(row["name"])): row
        for _, row in reference_df.iterrows()
    }
    loose_index: dict[tuple[str, str], list[pd.Series]] = {}
    for _, row in reference_df.iterrows():
        loose_index.setdefault((str(row["entity_type"]), str(row["name"])), []).append(row)

    descriptions = []
    urls = []
    categories = []
    synced_at_values = []

    for _, row in enriched.iterrows():
        key = (
            str(row.get("mongo_entity_type", "")),
            str(row.get("mongo_source_group", "")),
            str(row.get("mongo_name", "")),
        )
        match = exact_index.get(key)
        if match is None:
            loose_matches = loose_index.get((key[0], key[2]), [])
            if len(loose_matches) == 1:
                match = loose_matches[0]

        descriptions.append("" if match is None else str(match.get("short_description", "")))
        urls.append("" if match is None else str(match.get("doc_url", "")))
        categories.append("" if match is None else str(match.get("category", "")))
        synced_at_values.append("" if match is None else str(match.get("last_synced_at", "")))

    enriched["mongo_short_description"] = descriptions
    enriched["mongo_doc_url"] = urls
    enriched["mongo_reference_category"] = categories
    enriched["mongo_last_synced_at"] = synced_at_values
    return enriched
