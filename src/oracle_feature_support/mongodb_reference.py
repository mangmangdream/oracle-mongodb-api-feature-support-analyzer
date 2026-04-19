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
BASELINE_PATH = Path("outputs/mongodb_api_baseline.csv")
MAPPING_PATH = Path("outputs/oracle_compat_mapping.csv")
BASELINE_METADATA_PATH = Path("outputs/mongodb_api_baseline_metadata.json")
ABOUT_URL = "https://www.mongodb.com/docs/manual/about/"
DEFAULT_SUPPLEMENTAL_JSON_PATHS = [
    Path("/Users/qizou/MyDocs/OracleCorp/12c/23ai/JSON/mongodb-commands-and-operators_v2.1.json"),
]

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


@dataclass
class ApiBaselineBuildResult:
    baseline_df: pd.DataFrame
    mapping_df: pd.DataFrame
    metadata: dict[str, object]
    baseline_path: Path
    mapping_path: Path
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
    about_url: str = ABOUT_URL,
    source_overrides: dict[str, str] | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> ReferenceSyncResult:
    if progress_callback:
        progress_callback(f"[REF] Fetch manual version -> {about_url}")
    about_html = _fetch_html(
        about_url,
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
    source_pages: list[dict[str, str | int]] = [
        {
            "label": "MongoDB About",
            "url": about_url,
            "entry_count": 1 if manual_version else 0,
            "kind": "manual_version",
        }
    ]
    for source in MONGODB_REFERENCE_SOURCES:
        source_url = str((source_overrides or {}).get(source["source_group"], source["url"]))
        source_with_url = {**source, "url": source_url}
        if progress_callback:
            progress_callback(f"[REF] Sync source -> {source['label']}")
        html = _fetch_html(
            source_url,
            timeout=timeout,
            max_retries=max_retries,
            progress_callback=progress_callback,
        )
        extracted_rows = _extract_rows_from_source(html, source_with_url, progress_callback)
        all_rows.extend(extracted_rows)
        source_pages.append(
            {
                "label": source["label"],
                "url": source_url,
                "entry_count": int(len(extracted_rows)),
                "kind": str(source["source_group"]),
            }
        )

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
        "mongodb_manual_about_url": about_url,
        "source_pages": source_pages,
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


def _first_non_empty(values: list[object]) -> str:
    for value in values:
        text = _cell_text(value)
        if text:
            return text
    return ""


def _json_string(value: object) -> str:
    if not value:
        return ""
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        return ""


def _bool_text(value: object) -> str:
    return "true" if bool(value) else "false"


def _normalize_feature_type(
    feature_name: str,
    source_group: str = "",
    categories: list[dict[str, object]] | None = None,
) -> str:
    if source_group == "command":
        return "command"
    if source_group == "aggregation_stage":
        return "stage"
    if source_group:
        return "operator"

    if feature_name.startswith("$"):
        category_text = " | ".join(
            _clean_text(
                " ".join(
                    [
                        str(item.get("category_level_0", "") or ""),
                        str(item.get("category_level_1", "") or ""),
                    ]
                )
            )
            for item in (categories or [])
        ).lower()
        if "aggregation stage" in category_text or "aggregate() stages" in category_text:
            return "stage"
        return "operator"
    return "command"


def _normalize_source_group(
    feature_name: str,
    categories: list[dict[str, object]] | None = None,
) -> str:
    if not feature_name.startswith("$"):
        return "command"

    category_text = " | ".join(
        _clean_text(
            " ".join(
                [
                    str(item.get("category_level_0", "") or ""),
                    str(item.get("category_level_1", "") or ""),
                ]
            )
        )
        for item in (categories or [])
    ).lower()
    if "aggregation stages" in category_text or "aggregate() stages" in category_text:
        return "aggregation_stage"
    if "update operators" in category_text:
        return "update_operator"
    if "projection operators" in category_text:
        return "projection_operator"
    if "query and projection operators" in category_text or "query predicates" in category_text:
        return "query_operator"
    return "expression_operator"


def _coalesce_text_values(values: list[object]) -> str:
    cleaned = [_cell_text(value) for value in values]
    unique = [value for value in cleaned if value]
    deduped: list[str] = []
    for value in unique:
        if value not in deduped:
            deduped.append(value)
    return " | ".join(deduped)


def _load_supplemental_entries(
    supplemental_json_paths: list[Path] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    paths = supplemental_json_paths or DEFAULT_SUPPLEMENTAL_JSON_PATHS
    rows: list[dict[str, object]] = []
    used_files: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(raw, dict):
            continue
        used_files.append(str(path))
        for feature_name, payload in raw.items():
            if not isinstance(payload, dict):
                continue
            categories = payload.get("mongodb_doc_categories") or []
            feature_name_text = _clean_text(str(feature_name))
            if " " in feature_name_text:
                continue
            if not feature_name_text.startswith("$") and feature_name_text[:1].lower() != feature_name_text[:1]:
                continue
            source_group = _normalize_source_group(feature_name_text, categories)
            feature_type = _normalize_feature_type(
                feature_name_text,
                source_group=source_group,
                categories=categories,
            )
            details = payload.get("oracledb_api_for_mongodb_details")
            note_values: list[str] = []
            if isinstance(details, dict):
                notes = _cell_text(details.get("notes", ""))
                if notes and notes.lower() != "none":
                    note_values.append(notes)
            category_text = _coalesce_text_values(
                [
                    " / ".join(
                        [
                            _cell_text(item.get("category_level_0", "")),
                            _cell_text(item.get("category_level_1", "")),
                        ]
                    ).strip(" /")
                    for item in categories
                    if isinstance(item, dict)
                ]
            )
            if category_text:
                note_values.append(f"Supplemental categories: {category_text}")
            rows.append(
                {
                    "feature_type": feature_type,
                    "feature_name": feature_name_text,
                    "source_group": source_group,
                    "mongodb_category": category_text,
                    "mongo_short_description": "",
                    "mongo_doc_url": "",
                    "mongo_manual_version": "",
                    "baseline_source_kind": "supplemental_json",
                    "baseline_source_ref": str(path),
                    "admin_operational_flag": bool(payload.get("should_be_ignored_as_per_josh")),
                    "ignore_candidate_from_source": bool(payload.get("should_be_ignored_as_per_josh")),
                    "deprecated_flag": False,
                    "availability_notes": " | ".join(note_values),
                    "aliases_json": "",
                }
            )
    return pd.DataFrame(rows), used_files


def build_mongodb_api_baseline(
    reference_df: pd.DataFrame,
    manual_version: str = "",
    supplemental_json_paths: list[Path] | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    baseline_columns = [
        "feature_type",
        "feature_name",
        "source_group",
        "mongodb_category",
        "mongo_short_description",
        "mongo_doc_url",
        "mongo_manual_version",
        "baseline_source_kind",
        "baseline_source_ref",
        "admin_operational_flag",
        "ignore_candidate_from_source",
        "deprecated_flag",
        "availability_notes",
        "aliases_json",
    ]

    reference_rows: list[dict[str, object]] = []
    if not reference_df.empty:
        for _, row in reference_df.iterrows():
            feature_name = _cell_text(row.get("name", ""))
            if not feature_name:
                continue
            feature_type = _normalize_feature_type(
                feature_name,
                source_group=_cell_text(row.get("source_group", "")),
            )
            reference_rows.append(
                {
                    "feature_type": feature_type,
                    "feature_name": feature_name,
                    "source_group": _cell_text(row.get("source_group", "")),
                    "mongodb_category": _cell_text(row.get("category", "")),
                    "mongo_short_description": _cell_text(row.get("short_description", "")),
                    "mongo_doc_url": _cell_text(row.get("doc_url", "")),
                    "mongo_manual_version": _cell_text(manual_version),
                    "baseline_source_kind": "mongodb_docs",
                    "baseline_source_ref": _cell_text(row.get("source_page_url", "")),
                    "admin_operational_flag": False,
                    "ignore_candidate_from_source": False,
                    "deprecated_flag": False,
                    "availability_notes": "",
                    "aliases_json": "",
                }
            )
    reference_baseline_df = pd.DataFrame(reference_rows)
    if not reference_baseline_df.empty:
        reference_baseline_df = (
            reference_baseline_df.groupby(["feature_type", "feature_name"], dropna=False, as_index=False)
            .agg(
                source_group=("source_group", _coalesce_text_values),
                mongodb_category=("mongodb_category", _coalesce_text_values),
                mongo_short_description=("mongo_short_description", lambda values: _first_non_empty(list(values))),
                mongo_doc_url=("mongo_doc_url", lambda values: _first_non_empty(list(values))),
                mongo_manual_version=("mongo_manual_version", lambda values: _first_non_empty(list(values))),
                baseline_source_kind=("baseline_source_kind", lambda values: _first_non_empty(list(values))),
                baseline_source_ref=("baseline_source_ref", _coalesce_text_values),
                admin_operational_flag=("admin_operational_flag", any),
                ignore_candidate_from_source=("ignore_candidate_from_source", any),
                deprecated_flag=("deprecated_flag", any),
                availability_notes=("availability_notes", _coalesce_text_values),
                aliases_json=("aliases_json", lambda values: _first_non_empty(list(values))),
            )
        )

    supplemental_df, used_files = _load_supplemental_entries(supplemental_json_paths)
    combined_df = reference_baseline_df.copy()
    if combined_df.empty:
        combined_df = pd.DataFrame(columns=baseline_columns)
    if not supplemental_df.empty:
        if combined_df.empty:
            combined_df = supplemental_df.copy()
        else:
            combined_df = combined_df.merge(
                supplemental_df,
                on=["feature_type", "feature_name"],
                how="outer",
                suffixes=("", "_supp"),
            )
            for column in baseline_columns:
                if column in {"feature_type", "feature_name"}:
                    continue
                supp_column = f"{column}_supp"
                if supp_column not in combined_df.columns:
                    continue
                if column in {"admin_operational_flag", "ignore_candidate_from_source", "deprecated_flag"}:
                    left_values = combined_df[column].map(lambda value: bool(value) if pd.notna(value) else False)
                    right_values = combined_df[supp_column].map(lambda value: bool(value) if pd.notna(value) else False)
                    combined_df[column] = left_values | right_values
                elif column in {"source_group", "mongodb_category", "baseline_source_ref", "availability_notes"}:
                    combined_df[column] = combined_df.apply(
                        lambda row: _coalesce_text_values([row.get(column, ""), row.get(supp_column, "")]),
                        axis=1,
                    )
                elif column == "baseline_source_kind":
                    combined_df[column] = combined_df.apply(
                        lambda row: _coalesce_text_values([row.get(column, ""), row.get(supp_column, "")]),
                        axis=1,
                    )
                else:
                    combined_df[column] = combined_df[column].where(
                        combined_df[column].fillna("").astype(str).str.strip().ne(""),
                        combined_df[supp_column],
                    )
                combined_df = combined_df.drop(columns=[supp_column])

    if combined_df.empty:
        combined_df = pd.DataFrame(columns=baseline_columns)

    for column in baseline_columns:
        if column not in combined_df.columns:
            combined_df[column] = ""
    combined_df["mongo_manual_version"] = combined_df["mongo_manual_version"].where(
        combined_df["mongo_manual_version"].fillna("").astype(str).str.strip().ne(""),
        _cell_text(manual_version),
    )
    combined_df = combined_df[baseline_columns].sort_values(
        by=["feature_type", "feature_name"],
        kind="stable",
    ).reset_index(drop=True)
    combined_df["admin_operational_flag"] = combined_df["admin_operational_flag"].fillna(False).astype(bool)
    combined_df["ignore_candidate_from_source"] = combined_df["ignore_candidate_from_source"].fillna(False).astype(bool)
    combined_df["deprecated_flag"] = combined_df["deprecated_flag"].fillna(False).astype(bool)

    metadata: dict[str, object] = {
        "synced_at": datetime.now().isoformat(timespec="seconds"),
        "entry_count": int(len(combined_df)),
        "source_count": int(reference_df["source_group"].nunique()) if not reference_df.empty and "source_group" in reference_df.columns else 0,
        "supplemental_files": used_files,
        "supplemental_entry_count": int(len(supplemental_df)),
    }
    return combined_df, metadata


def build_oracle_compat_mapping(
    baseline_df: pd.DataFrame,
    detail_df: pd.DataFrame,
) -> pd.DataFrame:
    mapping_columns = [
        "feature_type",
        "feature_name",
        "oracle_matched",
        "oracle_match_type",
        "oracle_match_confidence",
        "oracle_section",
        "oracle_support_since",
        "oracle_support_raw",
        "oracle_uncovered_reason",
    ]
    if baseline_df.empty:
        return pd.DataFrame(columns=mapping_columns)

    oracle_rows: list[dict[str, object]] = []
    if not detail_df.empty:
        for _, row in detail_df.iterrows():
            feature_type, _, feature_name = infer_reference_key(row)
            feature_type = _cell_text(feature_type)
            feature_name = _cell_text(feature_name)
            if not feature_type or not feature_name:
                continue
            oracle_rows.append(
                {
                    "feature_type": feature_type,
                    "feature_name": feature_name,
                    "oracle_section": _cell_text(row.get("section", "")),
                    "oracle_support_since": _cell_text(row.get("Support (Since)", "")),
                    "oracle_support_raw": _cell_text(row.get("Support (Since)", "")),
                }
            )
    oracle_df = pd.DataFrame(oracle_rows).drop_duplicates().reset_index(drop=True)

    rows: list[dict[str, object]] = []
    if oracle_df.empty:
        for _, row in baseline_df.iterrows():
            rows.append(
                {
                    "feature_type": _cell_text(row.get("feature_type", "")),
                    "feature_name": _cell_text(row.get("feature_name", "")),
                    "oracle_matched": False,
                    "oracle_match_type": "unmatched",
                    "oracle_match_confidence": "0.00",
                    "oracle_section": "",
                    "oracle_support_since": "",
                    "oracle_support_raw": "",
                    "oracle_uncovered_reason": "oracle_feature_support_not_loaded",
                }
            )
        return pd.DataFrame(rows, columns=mapping_columns)

    oracle_lookup: dict[tuple[str, str], list[dict[str, object]]] = {}
    for _, row in oracle_df.iterrows():
        key = (_cell_text(row.get("feature_type", "")), _cell_text(row.get("feature_name", "")))
        oracle_lookup.setdefault(key, []).append(row.to_dict())

    for _, row in baseline_df.iterrows():
        key = (_cell_text(row.get("feature_type", "")), _cell_text(row.get("feature_name", "")))
        matches = oracle_lookup.get(key, [])
        if not matches:
            rows.append(
                {
                    "feature_type": key[0],
                    "feature_name": key[1],
                    "oracle_matched": False,
                    "oracle_match_type": "unmatched",
                    "oracle_match_confidence": "0.00",
                    "oracle_section": "",
                    "oracle_support_since": "",
                    "oracle_support_raw": "",
                    "oracle_uncovered_reason": "not_in_oracle_main_table",
                }
            )
            continue
        for match in matches:
            rows.append(
                {
                    "feature_type": key[0],
                    "feature_name": key[1],
                    "oracle_matched": True,
                    "oracle_match_type": "exact",
                    "oracle_match_confidence": "1.00",
                    "oracle_section": _cell_text(match.get("oracle_section", "")),
                    "oracle_support_since": _cell_text(match.get("oracle_support_since", "")),
                    "oracle_support_raw": _cell_text(match.get("oracle_support_raw", "")),
                    "oracle_uncovered_reason": "",
                }
            )

    return pd.DataFrame(rows, columns=mapping_columns).sort_values(
        by=["feature_type", "feature_name", "oracle_matched", "oracle_section", "oracle_support_since"],
        ascending=[True, True, False, True, True],
        kind="stable",
    ).reset_index(drop=True)


def build_mongodb_api_baseline_assets(
    reference_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    manual_version: str = "",
    supplemental_json_paths: list[Path] | None = None,
) -> ApiBaselineBuildResult:
    baseline_df, baseline_metadata = build_mongodb_api_baseline(
        reference_df=reference_df,
        manual_version=manual_version,
        supplemental_json_paths=supplemental_json_paths,
    )
    mapping_df = build_oracle_compat_mapping(
        baseline_df=baseline_df,
        detail_df=detail_df,
    )

    BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    baseline_df.assign(
        admin_operational_flag=baseline_df["admin_operational_flag"].map(_bool_text),
        ignore_candidate_from_source=baseline_df["ignore_candidate_from_source"].map(_bool_text),
        deprecated_flag=baseline_df["deprecated_flag"].map(_bool_text),
    ).to_csv(BASELINE_PATH, index=False, encoding="utf-8-sig")
    mapping_df.assign(
        oracle_matched=mapping_df["oracle_matched"].map(_bool_text),
    ).to_csv(MAPPING_PATH, index=False, encoding="utf-8-sig")

    metadata: dict[str, object] = {
        **baseline_metadata,
        "mapping_entry_count": int(len(mapping_df)),
        "oracle_matched_count": int(mapping_df["oracle_matched"].fillna(False).astype(bool).sum()) if not mapping_df.empty else 0,
        "oracle_uncovered_count": int((~mapping_df["oracle_matched"].fillna(False).astype(bool)).sum()) if not mapping_df.empty else 0,
        "baseline_path": str(BASELINE_PATH),
        "mapping_path": str(MAPPING_PATH),
    }
    BASELINE_METADATA_PATH.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return ApiBaselineBuildResult(
        baseline_df=baseline_df,
        mapping_df=mapping_df,
        metadata=metadata,
        baseline_path=BASELINE_PATH,
        mapping_path=MAPPING_PATH,
        metadata_path=BASELINE_METADATA_PATH,
    )


def load_mongodb_api_baseline() -> pd.DataFrame:
    if not BASELINE_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(BASELINE_PATH, dtype=str, keep_default_na=False)
        for column in ["admin_operational_flag", "ignore_candidate_from_source", "deprecated_flag"]:
            if column in df.columns:
                df[column] = df[column].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})
        return df
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


def load_oracle_compat_mapping() -> pd.DataFrame:
    if not MAPPING_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(MAPPING_PATH, dtype=str, keep_default_na=False)
        if "oracle_matched" in df.columns:
            df["oracle_matched"] = df["oracle_matched"].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})
        return df
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


def load_mongodb_api_baseline_metadata() -> dict[str, object]:
    if not BASELINE_METADATA_PATH.exists():
        return {}
    try:
        return json.loads(BASELINE_METADATA_PATH.read_text(encoding="utf-8"))
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
