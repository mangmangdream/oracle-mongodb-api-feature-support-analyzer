from __future__ import annotations

import altair as alt
import html as html_lib
import io
import json
import re
import shutil
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, unquote

import pandas as pd
import streamlit as st

from src.oracle_feature_support.feature_mapper import map_features_to_oracle_support
from src.oracle_feature_support.fetcher import (
    INDEX_URL,
    TARGET_URL,
    analyze_feature_support,
    write_feature_support_outputs,
)
from src.oracle_feature_support.mongodb_profile_reader import (
    read_system_profile,
    test_mongodb_connection,
)
from src.oracle_feature_support.mongodb_testkit import (
    run_profile_exercises,
    seed_test_data,
)
from src.oracle_feature_support.mongodb_reference import (
    ABOUT_URL,
    MONGODB_REFERENCE_SOURCES,
    enrich_feature_support_detail,
    load_mongodb_reference_catalog,
    load_mongodb_reference_metadata,
    sync_mongodb_reference_catalog,
)
from src.oracle_feature_support.migration_assessment import assess_migration_complexity
from src.oracle_feature_support.migration_rules import (
    ALLOWED_COMPLEXITY,
    load_migration_rules,
    save_customer_overrides,
)
from src.oracle_feature_support.profile_parser import (
    events_to_dataframe,
    extract_feature_usages,
    normalize_profile_records,
)
from src.oracle_feature_support.usage_report import (
    build_usage_summary,
    write_usage_analysis_outputs,
)

SETTINGS_PATH = Path("outputs/.ui_settings.json")
WORKSPACE_PANEL_HEIGHT = 1120

st.set_page_config(page_title="Oracle Database API for MongoDB Feature Support", layout="wide")
st.markdown(
    """
    <style>
    :root {
        --app-bg-top: #f2f7f7;
        --app-bg-bottom: #fbfdfd;
        --surface: #ffffff;
        --surface-soft: #f6faf9;
        --surface-tint: #eef6f5;
        --line: #d4e3e1;
        --line-strong: #c2d7d4;
        --text: #1f2937;
        --muted: #5d7277;
        --brand-deep: #0d3f47;
        --brand: #176b72;
        --brand-strong: #145c62;
        --brand-soft: #dff1ef;
        --shadow-soft: 0 14px 34px rgba(13, 63, 71, 0.08);
        --shadow-hover: 0 12px 26px rgba(23, 107, 114, 0.16);
    }
    div[data-testid="stAppViewContainer"] {
        background:
            radial-gradient(circle at top right, rgba(47, 143, 131, 0.1), transparent 28%),
            linear-gradient(180deg, var(--app-bg-top) 0%, var(--app-bg-bottom) 100%);
    }
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2.5rem;
        width: min(96vw, 1600px);
        max-width: 1600px;
    }
    .app-hero {
        background:
            radial-gradient(circle at top left, rgba(156, 227, 211, 0.22), transparent 24%),
            linear-gradient(135deg, #0b3942 0%, #135861 48%, #1c7b78 100%);
        border-radius: 8px;
        color: #ffffff;
        padding: 1.55rem 1.8rem;
        margin-bottom: 1.1rem;
        text-align: center;
        box-shadow: 0 18px 38px rgba(13, 63, 71, 0.18);
    }
    .app-hero h1 {
        font-size: 2rem;
        line-height: 1.2;
        margin: 0;
        letter-spacing: -0.02em;
    }
    .app-hero p {
        color: #d9f2ee;
        margin: 0;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background: rgba(255, 255, 255, 0.82);
        border-radius: 16px;
        box-shadow: var(--shadow-soft);
        backdrop-filter: blur(6px);
    }
    div[data-testid="stExpander"] {
        border: 1px solid var(--line);
        border-radius: 10px;
        background: var(--surface);
        box-shadow: 0 8px 18px rgba(13, 63, 71, 0.04);
    }
    div[data-testid="stExpander"] details summary p {
        font-weight: 700;
    }
    .config-sidecard {
        border: 1px solid var(--line);
        border-radius: 10px;
        background: linear-gradient(180deg, var(--surface-soft) 0%, var(--surface-tint) 100%);
        padding: 0.9rem 1rem;
        min-height: 100%;
    }
    .config-sidecard h4 {
        margin: 0 0 0.5rem 0;
        font-size: 1rem;
        color: var(--brand-deep);
    }
    .config-sidecard p {
        margin: 0 0 0.45rem 0;
        color: var(--muted);
        line-height: 1.5;
        font-size: 0.93rem;
    }
    .config-sidecard p:last-child {
        margin-bottom: 0;
    }
    .icon-toolbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.75rem;
        padding: 0.15rem 0 0.25rem 0;
    }
    .icon-toolbar-title {
        font-weight: 700;
        color: var(--text);
    }
    .icon-toolbar-actions {
        display: flex;
        align-items: center;
        gap: 0.4rem;
    }
    .panel-section-title {
        display: flex;
        align-items: center;
        margin: 0;
        min-height: 2.8rem;
        font-size: 1.18rem;
        line-height: 1.25;
        font-weight: 700;
        color: var(--text);
        letter-spacing: -0.01em;
    }
    .panel-anchor {
        display: block;
        height: 0;
        margin: 0;
        padding: 0;
        line-height: 0;
    }
    .panel-subsection-title {
        margin: 0;
        padding-top: 0.1rem;
        font-size: 1.02rem;
        line-height: 1.3;
        font-weight: 700;
        color: #31434a;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(180deg, #fbfdfd 0%, #f4f8f8 100%);
        border: 1px solid #dce9e7;
        border-radius: 12px;
        padding: 0.72rem 0.82rem;
        box-shadow: 0 8px 18px rgba(13, 63, 71, 0.04);
    }
    div[data-testid="stMetric"] label,
    div[data-testid="stMetricLabel"] {
        font-size: 0.82rem !important;
        line-height: 1.15 !important;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.2rem !important;
        line-height: 1.1 !important;
    }
    div[data-testid="stMetric"] * {
        word-break: normal;
    }
    div[data-testid="stMetric"] label p,
    div[data-testid="stMetricValue"] > div {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .stButton > button,
    .stFormSubmitButton > button,
    .stDownloadButton > button {
        min-height: 2.6rem;
        border-radius: 10px;
        border: 1px solid var(--line-strong);
        background: linear-gradient(180deg, #f9fcfc 0%, #eef6f5 100%);
        color: var(--brand-deep);
        font-weight: 700;
        white-space: nowrap;
        transition: background 0.18s ease, border-color 0.18s ease, color 0.18s ease, box-shadow 0.18s ease, transform 0.18s ease;
    }
    .stButton > button:hover,
    .stFormSubmitButton > button:hover,
    .stDownloadButton > button:hover {
        background: linear-gradient(135deg, #176b72 0%, #2b8a82 100%);
        border-color: #176b72;
        color: #ffffff;
        box-shadow: var(--shadow-hover);
        transform: translateY(-1px);
    }
    .stButton > button[kind="primary"],
    .stFormSubmitButton > button[kind="primary"],
    .stDownloadButton > button[kind="primary"] {
        background: linear-gradient(135deg, var(--brand-deep) 0%, var(--brand) 100%);
        color: #ffffff;
        border-color: var(--brand-deep);
    }
    .stButton > button[kind="primary"]:hover,
    .stFormSubmitButton > button[kind="primary"]:hover,
    .stDownloadButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #082f36 0%, var(--brand-strong) 100%);
        color: #ffffff;
    }
    .stButton > button:focus-visible,
    .stFormSubmitButton > button:focus-visible,
    .stDownloadButton > button:focus-visible {
        outline: 2px solid rgba(23, 107, 114, 0.28);
        outline-offset: 2px;
        box-shadow: 0 0 0 4px rgba(23, 107, 114, 0.14);
    }
    .stButton > button:disabled,
    .stFormSubmitButton > button:disabled,
    .stDownloadButton > button:disabled {
        opacity: 0.58;
        box-shadow: none;
        transform: none;
        cursor: not-allowed;
    }
    div[data-testid="stDataFrame"] {
        border-radius: 10px;
        border: 1px solid #e0ebea;
        overflow: hidden;
    }
    div[data-testid="stAlert"] {
        border-radius: 12px;
        border: 1px solid var(--line);
        box-shadow: 0 8px 18px rgba(13, 63, 71, 0.04);
    }
    div[data-baseweb="tab-list"] {
        gap: 0.4rem;
        align-items: stretch;
    }
    button[data-baseweb="tab"] {
        min-height: 2.6rem;
        padding: 0.4rem 1rem;
        border-radius: 12px;
        border: 1px solid #d8e6e4;
        background: rgba(255, 255, 255, 0.72);
        color: var(--muted);
        font-size: 0.95rem;
        line-height: 1.2;
        font-weight: 700;
        transition: background 0.18s ease, color 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
    }
    button[data-baseweb="tab"]:hover {
        background: rgba(223, 241, 239, 0.9);
        color: var(--brand-deep);
        border-color: #c9e1dd;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(180deg, #edf7f6 0%, #dff1ef 100%);
        border-color: #c9e1dd;
        color: var(--brand-deep);
        box-shadow: 0 8px 18px rgba(13, 63, 71, 0.06);
    }
    </style>
    <div class="app-hero">
        <h1>Oracle Database API for MongoDB Feature Support</h1>
    </div>
    """,
    unsafe_allow_html=True,
)

if "result_detail_df" not in st.session_state:
    st.session_state.result_detail_df = None
if "result_summary_df" not in st.session_state:
    st.session_state.result_summary_df = None
if "result_output_dir" not in st.session_state:
    st.session_state.result_output_dir = None
if "doc_metadata" not in st.session_state:
    st.session_state.doc_metadata = {}
if "reference_df" not in st.session_state:
    st.session_state.reference_df = pd.DataFrame()
if "reference_metadata" not in st.session_state:
    st.session_state.reference_metadata = {}
if "debug_logs" not in st.session_state:
    st.session_state.debug_logs = []
if "restore_attempted" not in st.session_state:
    st.session_state.restore_attempted = False
if "restored_from_disk" not in st.session_state:
    st.session_state.restored_from_disk = False
if "mongo_usage_restore_attempted" not in st.session_state:
    st.session_state.mongo_usage_restore_attempted = False
if "mongo_usage_restored_from_disk" not in st.session_state:
    st.session_state.mongo_usage_restored_from_disk = False
if "oracle_cache_save_notice" not in st.session_state:
    st.session_state.oracle_cache_save_notice = ""
if "oracle_selected_cache_dir" not in st.session_state:
    st.session_state.oracle_selected_cache_dir = ""
if "confirm_clear_oracle_caches" not in st.session_state:
    st.session_state.confirm_clear_oracle_caches = False
if "mongo_usage_cache_save_notice" not in st.session_state:
    st.session_state.mongo_usage_cache_save_notice = ""
if "settings_loaded" not in st.session_state:
    st.session_state.settings_loaded = False
if "detail_expanded_sections" not in st.session_state:
    st.session_state.detail_expanded_sections = {}
if "support_analysis_expanded" not in st.session_state:
    st.session_state.support_analysis_expanded = True
if "feature_detail_expanded" not in st.session_state:
    st.session_state.feature_detail_expanded = False
if "mongo_usage_detail_df" not in st.session_state:
    st.session_state.mongo_usage_detail_df = None
if "mongo_usage_summary_df" not in st.session_state:
    st.session_state.mongo_usage_summary_df = None
if "mongo_usage_events_df" not in st.session_state:
    st.session_state.mongo_usage_events_df = None
if "mongo_usage_output_dir" not in st.session_state:
    st.session_state.mongo_usage_output_dir = None
if "mongo_usage_metadata" not in st.session_state:
    st.session_state.mongo_usage_metadata = {}
if "mongo_usage_migration_summary_df" not in st.session_state:
    st.session_state.mongo_usage_migration_summary_df = None
if "mongo_usage_baseline_df" not in st.session_state:
    st.session_state.mongo_usage_baseline_df = None
if "mongo_usage_hotspots_df" not in st.session_state:
    st.session_state.mongo_usage_hotspots_df = None
if "mongo_usage_excluded_df" not in st.session_state:
    st.session_state.mongo_usage_excluded_df = None
if "mongo_usage_override_save_notice" not in st.session_state:
    st.session_state.mongo_usage_override_save_notice = ""
if "mongo_usage_trace_logs" not in st.session_state:
    st.session_state.mongo_usage_trace_logs = []
if "mongo_usage_connection_test" not in st.session_state:
    st.session_state.mongo_usage_connection_test = {}
if "mongo_testkit_trace_logs" not in st.session_state:
    st.session_state.mongo_testkit_trace_logs = []
if "mongo_testkit_connection_test" not in st.session_state:
    st.session_state.mongo_testkit_connection_test = {}
if "mongo_testkit_seed_result" not in st.session_state:
    st.session_state.mongo_testkit_seed_result = {}
if "mongo_testkit_exercise_result" not in st.session_state:
    st.session_state.mongo_testkit_exercise_result = {}
if "mongo_usage_selected_cache_dir" not in st.session_state:
    st.session_state.mongo_usage_selected_cache_dir = ""
if "confirm_clear_usage_caches" not in st.session_state:
    st.session_state.confirm_clear_usage_caches = False
if "feature_detail_target_version" not in st.session_state:
    st.session_state.feature_detail_target_version = "任意版本"
if "feature_detail_target_mode" not in st.session_state:
    st.session_state.feature_detail_target_mode = "任意部署方式"
if "usage_analysis_target_version" not in st.session_state:
    st.session_state.usage_analysis_target_version = "任意版本"
if "usage_analysis_target_mode" not in st.session_state:
    st.session_state.usage_analysis_target_mode = "任意部署方式"
if "usage_complexity_filter" not in st.session_state:
    st.session_state.usage_complexity_filter = []
if "usage_scope_filter" not in st.session_state:
    st.session_state.usage_scope_filter = []
if "usage_baseline_show_advanced_columns" not in st.session_state:
    st.session_state.usage_baseline_show_advanced_columns = False


def _parse_optional_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"无法解析时间: {value}。请使用 ISO 8601，例如 2026-04-10T12:30:00") from exc


def _resolve_mongodb_database_name(mongodb_uri: str, database_name_input: str) -> str:
    explicit_name = str(database_name_input or "").strip()
    if explicit_name:
        return explicit_name

    parsed = urlparse(str(mongodb_uri or "").strip())
    path = str(parsed.path or "").strip()
    if path and path != "/":
        return unquote(path.lstrip("/")).strip()
    return ""


def _normalize_oracle_version(value: object) -> str:
    return str(value or "").strip().lower().replace(" ", "")


def _normalize_support_mode(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"any", "任意部署方式", "任意"}:
        return "any"
    if normalized in {"noop", "no-op", "no_op"}:
        return "no-op"
    return "op"


def _oracle_version_rank(value: object) -> tuple[int, int] | None:
    normalized = _normalize_oracle_version(value)
    match = re.fullmatch(r"(\d+)(ai|c)", normalized)
    if not match:
        return None
    number = int(match.group(1))
    suffix = match.group(2)
    return (number, 1 if suffix == "ai" else 0)


def _extract_oracle_versions(raw_value: object) -> list[str]:
    tokens = re.findall(r"\b(\d+\s*(?:ai|c))\b", str(raw_value or ""), flags=re.IGNORECASE)
    normalized: list[str] = []
    for token in tokens:
        version = _normalize_oracle_version(token)
        if version and version not in normalized:
            normalized.append(version)
    return normalized


def _available_oracle_versions(*series_values: object) -> list[str]:
    found: list[str] = []
    for values in series_values:
        if values is None:
            continue
        for value in values:
            for version in _extract_oracle_versions(value):
                if version not in found:
                    found.append(version)
    if not found:
        found = ["19c", "26ai"]
    found.sort(key=lambda item: _oracle_version_rank(item) or (0, 0))
    return found


def _effective_oracle_support_status(
    support_since_value: object,
    target_version: str,
    target_mode: str,
) -> str:
    text = str(support_since_value or "").strip()
    if not text:
        return "Unknown"

    low = text.lower()
    if any(key in low for key in ["not supported", "unsupported", "not applicable"]) or low in {"no", "n/a", "na"}:
        return "Not Supported"

    candidate_versions = _extract_oracle_versions(text) or ["19c", "26ai"]
    normalized_target_version = str(target_version or "").strip()
    if normalized_target_version in {"", "任意版本", "任意"}:
        versions_to_check = candidate_versions
    else:
        versions_to_check = [_normalize_oracle_version(normalized_target_version)]

    target_mode_normalized = _normalize_support_mode(target_mode)
    modes_to_check = ["op", "no-op"] if target_mode_normalized == "any" else [target_mode_normalized]

    parenthesized_segments = re.findall(r"\(([^)]*)\)", text)
    no_op_only_versions: list[str] = []
    if "no-op" in low:
        for segment in parenthesized_segments:
            for version in _extract_oracle_versions(segment):
                if version not in no_op_only_versions:
                    no_op_only_versions.append(version)

    base_text = re.sub(r"\([^)]*\)", " ", text)
    base_versions = _extract_oracle_versions(base_text)

    for candidate_version in versions_to_check:
        candidate_rank = _oracle_version_rank(candidate_version)
        for candidate_mode in modes_to_check:
            if low == "no-op":
                if candidate_mode == "no-op" and candidate_version in {"19c", "26ai"}:
                    return "Supported"
                continue

            if candidate_mode == "no-op" and candidate_version in no_op_only_versions:
                return "Supported"

            if base_versions and candidate_rank is not None:
                ranked_versions = [
                    _oracle_version_rank(version)
                    for version in base_versions
                    if _oracle_version_rank(version) is not None
                ]
                if ranked_versions:
                    min_required_rank = min(ranked_versions)
                    if candidate_rank >= min_required_rank:
                        return "Supported"

    return "Not Supported"


def _render_oracle_target_controls(
    version_key: str,
    mode_key: str,
    version_options: list[str],
    caption_text: str,
) -> tuple[str, str]:
    version_select_options = ["任意版本", *version_options]
    mode_select_options = ["任意部署方式", "op", "no-op"]

    current_version = str(st.session_state.get(version_key, "任意版本") or "").strip()
    if current_version not in version_select_options:
        current_version = "任意版本"
    current_mode = str(st.session_state.get(mode_key, "任意部署方式") or "").strip()
    if current_mode not in mode_select_options:
        current_mode = "任意部署方式"

    if version_key not in st.session_state:
        st.session_state[version_key] = current_version
    if mode_key not in st.session_state:
        st.session_state[mode_key] = current_mode

    target_cols = st.columns(2, gap="medium")
    with target_cols[0]:
        selected_version = st.selectbox(
            "目标 Oracle 数据库版本",
            options=version_select_options,
            key=version_key,
            help="按所选 Oracle 版本重新计算当前区域里的支持状态。",
        )
    with target_cols[1]:
        selected_mode = st.selectbox(
            "部署方式",
            options=mode_select_options,
            key=mode_key,
            help="OP 表示标准支持路径，No-op 表示仅在 no-op 模式下支持。",
        )
    st.caption(caption_text)

    return str(selected_version).strip(), _normalize_support_mode(selected_mode)


def _oracle_target_mode_label(value: str) -> str:
    normalized = _normalize_support_mode(value)
    if normalized == "any":
        return "任意部署方式"
    return normalized


def _save_ui_settings() -> None:
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(
        json.dumps(
            {
                "url": st.session_state.ui_saved_url,
                "mongodb_url": st.session_state.ui_saved_mongodb_url,
                "doc_link_urls": st.session_state.ui_doc_link_urls,
                "default_doc_link_urls": st.session_state.ui_default_doc_link_urls,
                "timeout": int(st.session_state.ui_timeout),
                "max_retries": int(st.session_state.ui_max_retries),
                "show_debug_log": bool(st.session_state.ui_show_debug),
                "mongo_usage_uri": str(st.session_state.get("mongo_usage_uri", "") or ""),
                "mongo_testkit_uri": str(st.session_state.get("mongo_testkit_uri", "") or ""),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


if not st.session_state.settings_loaded:
    saved_settings = {}
    if SETTINGS_PATH.exists():
        try:
            saved_settings = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            saved_settings = {}
    default_doc_link_urls = {
        "oracle_feature_support": TARGET_URL,
        "oracle_version_source": INDEX_URL,
        "manual_version": ABOUT_URL,
        **{source["source_group"]: source["url"] for source in MONGODB_REFERENCE_SOURCES},
    }
    saved_default_doc_link_urls = saved_settings.get("default_doc_link_urls", {})
    if isinstance(saved_default_doc_link_urls, dict):
        for key, value in saved_default_doc_link_urls.items():
            if key in default_doc_link_urls and str(value).strip():
                default_doc_link_urls[key] = str(value).strip()
    saved_doc_link_urls = saved_settings.get("doc_link_urls", {})
    doc_link_urls = default_doc_link_urls.copy()
    if isinstance(saved_doc_link_urls, dict):
        for key, value in saved_doc_link_urls.items():
            if key in doc_link_urls and str(value).strip():
                doc_link_urls[key] = str(value).strip()
    else:
        saved_url = str(saved_settings.get("url", TARGET_URL)).strip()
        saved_mongodb_url = str(saved_settings.get("mongodb_url", ABOUT_URL)).strip()
        doc_link_urls["oracle_feature_support"] = saved_url or TARGET_URL
        doc_link_urls["manual_version"] = saved_mongodb_url or ABOUT_URL
    st.session_state.ui_default_doc_link_urls = default_doc_link_urls
    st.session_state.ui_doc_link_urls = doc_link_urls
    st.session_state.ui_saved_url = doc_link_urls["oracle_feature_support"]
    st.session_state.ui_saved_mongodb_url = doc_link_urls["manual_version"]
    st.session_state.ui_timeout = int(saved_settings.get("timeout", 60))
    st.session_state.ui_max_retries = int(saved_settings.get("max_retries", 3))
    st.session_state.ui_show_debug = bool(saved_settings.get("show_debug_log", True))
    st.session_state.mongo_usage_uri = str(saved_settings.get("mongo_usage_uri", "") or "")
    st.session_state.mongo_testkit_uri = str(saved_settings.get("mongo_testkit_uri", "") or "")
    st.session_state.settings_loaded = True

if st.session_state.reference_df.empty:
    st.session_state.reference_df = load_mongodb_reference_catalog()
if not st.session_state.reference_metadata:
    st.session_state.reference_metadata = load_mongodb_reference_metadata()


def _mongodb_source_pages(metadata: dict[str, object]) -> list[dict[str, object]]:
    source_pages = metadata.get("source_pages")
    if isinstance(source_pages, list) and source_pages:
        return [
            page for page in source_pages
            if isinstance(page, dict) and str(page.get("url", "")).strip()
        ]

    fallback_pages: list[dict[str, object]] = []
    about_url = str(metadata.get("mongodb_manual_about_url", "")).strip()
    if about_url:
        fallback_pages.append(
            {
                "label": "MongoDB About",
                "url": about_url,
                "entry_count": 1 if str(metadata.get("mongodb_manual_version", "")).strip() else 0,
                "kind": "manual_version",
            }
        )
    for source in MONGODB_REFERENCE_SOURCES:
        fallback_pages.append(
            {
                "label": source["label"],
                "url": source["url"],
                "entry_count": "",
                "kind": source["source_group"],
            }
        )
    return fallback_pages


def _document_link_definitions() -> list[dict[str, str]]:
    definitions = [
        {
            "id": "oracle_feature_support",
            "label": "Oracle Feature Support 文档",
            "description": "用于同步 Oracle Feature Support 明细",
            "default_url": TARGET_URL,
        },
        {
            "id": "oracle_version_source",
            "label": "Oracle 文档版本页",
            "description": "用于确认 Oracle 文档版本信息",
            "default_url": INDEX_URL,
        },
        {
            "id": "manual_version",
            "label": "MongoDB 文档版本页",
            "description": "用于确认 MongoDB 文档版本号",
            "default_url": ABOUT_URL,
        },
    ]
    for source in MONGODB_REFERENCE_SOURCES:
        definitions.append(
            {
                "id": source["source_group"],
                "label": source["label"],
                "description": f"用于同步 MongoDB {source['label']} 说明",
                "default_url": source["url"],
            }
        )
    return definitions


def _document_link_rows(
    metadata: dict[str, object],
    doc_link_urls: dict[str, str],
    oracle_entry_count: int | None = None,
) -> list[dict[str, object]]:
    source_page_map = {
        str(page.get("kind", "")): page
        for page in _mongodb_source_pages(metadata)
        if isinstance(page, dict)
    }
    rows: list[dict[str, object]] = []
    for definition in _document_link_definitions():
        page = source_page_map.get(definition["id"], {})
        entry_count = page.get("entry_count", "")
        if definition["id"] == "oracle_feature_support" and oracle_entry_count is not None:
            entry_count = oracle_entry_count
        rows.append(
            {
                "id": definition["id"],
                "文档项": definition["label"],
                "链接": doc_link_urls.get(definition["id"], definition["default_url"]),
                "上次抓取数": entry_count,
                "用途说明": definition["description"],
            }
        )
    return rows


def _displayable_mongodb_source_pages(
    metadata: dict[str, object],
    current_mongodb_url: str,
) -> list[dict[str, object]]:
    current_url = current_mongodb_url.strip()
    return [
        page
        for page in _mongodb_source_pages(metadata)
        if str(page.get("url", "")).strip()
        and str(page.get("url", "")).strip() != current_url
    ]


def _dataframe_height(row_count: int, min_height: int = 72, max_height: int = 320) -> int:
    header_height = 38
    row_height = 35
    padding = 14
    computed = header_height + max(row_count, 1) * row_height + padding
    return max(min_height, min(computed, max_height))


def _build_excel_workbook_bytes(sheets: list[tuple[str, pd.DataFrame]]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets:
            safe_sheet_name = str(sheet_name or "Sheet")[:31]
            df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
    output.seek(0)
    return output.getvalue()


def _prune_output_dirs(pattern: str, keep: int = 1, output_root: str = "outputs") -> None:
    root = Path(output_root)
    if not root.exists():
        return
    candidates = sorted(
        [path for path in root.glob(pattern) if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for stale_dir in candidates[max(keep, 0):]:
        shutil.rmtree(stale_dir, ignore_errors=True)


def _list_output_dirs(pattern: str, output_root: str = "outputs") -> list[Path]:
    root = Path(output_root)
    if not root.exists():
        return []
    return sorted(
        [path for path in root.glob(pattern) if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _clear_output_dirs(pattern: str, output_root: str = "outputs") -> int:
    removed_count = 0
    for cache_dir in _list_output_dirs(pattern, output_root=output_root):
        shutil.rmtree(cache_dir, ignore_errors=True)
        removed_count += 1
    return removed_count


def _persist_oracle_cache(
    detail_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    doc_metadata: dict[str, object],
    report_html: str,
    workbook_bytes: bytes,
) -> Path:
    output_dir = write_feature_support_outputs(
        detail_df=detail_df,
        summary_df=summary_df,
        doc_metadata=doc_metadata,
    )
    (output_dir / "feature_support_report.html").write_text(report_html, encoding="utf-8")
    (output_dir / "feature_support_analysis.xlsx").write_bytes(workbook_bytes)
    _prune_output_dirs("feature_support_*", keep=10)
    return output_dir


def _persist_usage_cache(
    detail_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    metadata: dict[str, object],
    report_html: str,
    workbook_bytes: bytes,
    migration_summary_df: pd.DataFrame | None = None,
    hotspots_df: pd.DataFrame | None = None,
    excluded_df: pd.DataFrame | None = None,
) -> Path:
    artifacts = write_usage_analysis_outputs(
        detail_df=detail_df,
        summary_df=summary_df,
        metadata=metadata,
        migration_detail_df=detail_df,
        migration_summary_df=migration_summary_df,
        migration_hotspots_df=hotspots_df,
        migration_excluded_df=excluded_df,
    )
    (artifacts.output_dir / "mongodb_usage_report.html").write_text(report_html, encoding="utf-8")
    (artifacts.output_dir / "mongodb_usage_analysis.xlsx").write_bytes(workbook_bytes)
    _prune_output_dirs("mongodb_usage_*", keep=10)
    return artifacts.output_dir


def _latest_oracle_entry_count(output_root: str = "outputs") -> int | None:
    root = Path(output_root)
    if not root.exists():
        return None
    candidates = [path for path in root.glob("feature_support_*") if path.is_dir()]
    if not candidates:
        return None
    latest_dir = sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)[0]
    detail_path = latest_dir / "feature_support_detail.csv"
    if not detail_path.exists():
        return None
    try:
        with detail_path.open("r", encoding="utf-8-sig") as handle:
            row_count = sum(1 for _ in handle) - 1
        return max(row_count, 0)
    except Exception:  # noqa: BLE001
        return None


doc_tab, usage_tab, testkit_tab = st.tabs(["文档同步", "MongoDB Usage 分析", "MongoDB 测试工具"])

with doc_tab:
    doc_left_col, doc_right_col = st.columns([1.0, 2.15], gap="large")
    with doc_left_col:
        doc_left_panel = st.container(height=WORKSPACE_PANEL_HEIGHT, border=False)
    with doc_right_col:
        doc_right_panel = st.container(height=WORKSPACE_PANEL_HEIGHT, border=False)
    with doc_right_panel:
        log_container = st.empty()

with doc_left_panel:
    with st.container(border=True):
        st.markdown("#### 同步配置")
        st.markdown("##### 数据源配置")
        if "doc_links_expanded" not in st.session_state:
            st.session_state.doc_links_expanded = False
        header_cols = st.columns([1.0, 0.42], gap="small")
        with header_cols[0]:
            st.markdown('<div class="icon-toolbar-title" style="padding-top:0.35rem;">文档链接与来源</div>', unsafe_allow_html=True)
        with header_cols[1]:
            toolbar_cols = st.columns(3, gap="small")
            with toolbar_cols[0]:
                save_url = st.button(
                    "",
                    key="save_doc_links_default",
                    help="将当前链接保存为默认 URL",
                    icon=":material/star:",
                    width="stretch",
                )
            with toolbar_cols[1]:
                restore_default_url = st.button(
                    "",
                    key="restore_doc_links_default",
                    help="恢复默认 URL",
                    icon=":material/restart_alt:",
                    width="stretch",
                )
            with toolbar_cols[2]:
                toggle_doc_links = st.button(
                    "",
                    key="toggle_doc_links",
                    help="展开或折叠文档链接与来源",
                    icon=":material/expand_more:" if st.session_state.doc_links_expanded else ":material/chevron_right:",
                    width="stretch",
                )
        if toggle_doc_links:
            st.session_state.doc_links_expanded = not st.session_state.doc_links_expanded
            st.rerun()
        if st.session_state.doc_links_expanded:
            source_rows = _document_link_rows(
                st.session_state.reference_metadata or {},
                st.session_state.ui_doc_link_urls,
                oracle_entry_count=(
                    len(st.session_state.result_detail_df)
                    if st.session_state.result_detail_df is not None
                    else _latest_oracle_entry_count()
                ),
            )
            if source_rows:
                edited_source_values: dict[str, str] = {}
                for row in source_rows:
                    row_id = str(row["id"])
                    input_key = f"doc_link_input_{row_id}"
                    current_value = str(
                        st.session_state.ui_doc_link_urls.get(row_id, row["链接"])
                    ).strip()
                    if input_key not in st.session_state:
                        st.session_state[input_key] = current_value
                    fetch_count = row["上次抓取数"]
                    fetch_count_label = (
                        str(int(fetch_count))
                        if str(fetch_count).strip() not in {"", "None"}
                        else "-"
                    )
                    with st.container(border=True):
                        st.markdown(f"**{row['文档项']}**")
                        st.caption(f"上次抓取数: {fetch_count_label}")
                        st.text_input(
                            "链接",
                            key=input_key,
                            label_visibility="collapsed",
                        )
                        st.caption(str(row["用途说明"]))
                        edited_source_values[row_id] = str(st.session_state.get(input_key, "")).strip()
                edited_source_df = edited_source_values
            else:
                edited_source_df = None
                st.caption("暂无文档链接。")
        else:
            edited_source_df = None

        st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)
        st.markdown("##### 同步执行")
        with st.form("run_form", clear_on_submit=False):
            form_cols = st.columns(2, gap="medium")
            with form_cols[0]:
                timeout = st.number_input(
                    "请求超时（秒）", min_value=10, max_value=180, step=5, key="ui_timeout"
                )
            with form_cols[1]:
                max_retries = st.number_input(
                    "重试次数", min_value=0, max_value=6, step=1, key="ui_max_retries"
                )
            show_debug_log = st.checkbox("显示执行日志（排查问题）", key="ui_show_debug")
            st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)
            sync_reference = st.form_submit_button("同步 MongoDB 官方说明", width="stretch")
            submitted = st.form_submit_button("同步 Oracle 官方文档", type="primary", width="stretch")
            st.markdown("<div style='height:0.35rem'></div>", unsafe_allow_html=True)
            reference_meta = st.session_state.reference_metadata or {}
            cache_summary = (
                f"{reference_meta.get('entry_count', 0)} 条，上次同步 {reference_meta.get('synced_at', '未知')}"
                if reference_meta
                else "尚未同步"
            )
            st.markdown(
                f"""
                <div class="config-sidecard">
                  <h4>同步说明</h4>
                  <p>说明缓存：{cache_summary}</p>
                  <p>Oracle 文档同步不会自动更新 MongoDB 官方说明缓存。</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

    if edited_source_df is not None:
        updated_doc_link_urls = st.session_state.ui_doc_link_urls.copy()
        changed = False
        for definition in _document_link_definitions():
            new_value = str(edited_source_df.get(definition["id"], "")).strip() or definition["default_url"]
            if updated_doc_link_urls.get(definition["id"]) != new_value:
                updated_doc_link_urls[definition["id"]] = new_value
                changed = True
        if changed:
            st.session_state.ui_doc_link_urls = updated_doc_link_urls
            st.session_state.ui_saved_url = updated_doc_link_urls["oracle_feature_support"]
            st.session_state.ui_saved_mongodb_url = updated_doc_link_urls["manual_version"]
            _save_ui_settings()

    if save_url:
        st.session_state.ui_default_doc_link_urls = st.session_state.ui_doc_link_urls.copy()
        _save_ui_settings()
        st.rerun()
    elif restore_default_url:
        st.session_state.ui_doc_link_urls = {
            key: value for key, value in st.session_state.ui_default_doc_link_urls.items()
        }
        st.session_state.ui_saved_url = st.session_state.ui_doc_link_urls["oracle_feature_support"]
        st.session_state.ui_saved_mongodb_url = st.session_state.ui_doc_link_urls["manual_version"]
        for definition in _document_link_definitions():
            st.session_state.pop(f"doc_link_input_{definition['id']}", None)
        _save_ui_settings()
        st.rerun()


def _drop_empty_columns(df):
    filtered = df.copy()
    keep_cols = []
    for col in filtered.columns:
        series = filtered[col].fillna("").astype(str).str.strip()
        if series.ne("").any():
            keep_cols.append(col)
    return filtered[keep_cols]


def _drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    filtered = df.copy()
    non_empty_mask = filtered.fillna("").astype(str).apply(
        lambda row: row.str.strip().ne("").any(),
        axis=1,
    )
    return filtered[non_empty_mask].copy()


def _section_inline_bar_html(width_percent: float, color: str) -> str:
    width = max(min(width_percent, 100.0), 2.0)
    return f"""
    <div style="width:100%; padding-top:0.15rem;">
      <div style="width:100%; height:12px; border-radius:999px; background:#e7eef0; overflow:hidden;">
        <div style="width:{width:.2f}%; height:100%; border-radius:999px; background:{color};"></div>
      </div>
    </div>
    """


def _status_segments_from_df(df: pd.DataFrame) -> list[tuple[str, int, str]]:
    color_map = {
        "Supported": "#2e8b57",
        "Not Supported": "#d9534f",
        "Partially Supported": "#f0ad4e",
        "Other": "#6c757d",
    }
    if "normalized_status" not in df.columns or df.empty:
        return [("Other", 0, color_map["Other"])]

    counts = (
        df["normalized_status"]
        .fillna("Other")
        .astype(str)
        .replace("", "Other")
        .value_counts()
    )
    ordered_statuses = ["Supported", "Not Supported", "Partially Supported", "Other"]
    segments: list[tuple[str, int, str]] = []
    for status in ordered_statuses:
        count = int(counts.get(status, 0))
        if count:
            segments.append((status, count, color_map[status]))
    extra_count = int(
        counts[[status for status in counts.index if status not in ordered_statuses]].sum()
    ) if not counts.empty else 0
    if extra_count:
        segments.append(("Other", extra_count, color_map["Other"]))
    return segments or [("Other", 0, color_map["Other"])]


def _section_status_bar_html(df: pd.DataFrame) -> str:
    total = max(len(df), 1)
    segments = _status_segments_from_df(df)
    parts: list[str] = []
    for status, count, color in segments:
        width = max((count / total) * 100, 2.0) if count else 0
        if not count:
            continue
        parts.append(
            f"<span title='{html_lib.escape(status)}: {count}' "
            f"style='display:block;height:100%;width:{width:.2f}%;background:{color};'></span>"
        )
    return (
        "<div style='width:100%;'>"
        "<div style='display:flex;width:100%;height:10px;border-radius:999px;background:#e7eef0;overflow:hidden;'>"
        + "".join(parts)
        + "</div></div>"
    )


def _pick_existing_columns(cols: list[str], candidates: list[str]) -> list[str]:
    picked: list[str] = []
    for candidate in candidates:
        for col in cols:
            if col == candidate and col not in picked:
                picked.append(col)
    return picked


def _reorder_detail_columns(df: pd.DataFrame, section_name: str | None = None) -> pd.DataFrame:
    cols = list(df.columns)
    section_text = (section_name or "").strip().lower()

    section_specific_front: list[str] = []
    if "data types" in section_text:
        section_specific_front = [
            "Data Types",
            "Data Type",
            "Data Type and Alias",
            "BSON Type",
            "Alias",
        ]
    elif "index options" in section_text:
        section_specific_front = [
            "Index Option",
            "Index option",
        ]
    elif "index" in section_text:
        section_specific_front = [
            "Index Type",
            "Index type",
        ]

    generic_front = [
        "Command",
        "Operator",
        "Stage",
        "Feature",
        "支持判断",
        "Data Type and Alias",
        "Data Type",
        "Index Type",
        "Index Option",
        "Field",
        "Fields",
        "Expression",
        "Accumulator",
        "Option",
        "Options",
        "Type",
        "Name",
        "Support (Since)",
    ]
    generic_tail = [
        "功能说明",
        "MongoDB 官方文档",
        "Notes",
        "Note",
    ]

    first_cols = _pick_existing_columns(cols, section_specific_front + generic_front)
    tail_cols = _pick_existing_columns(cols, generic_tail)
    remaining = [col for col in cols if col not in first_cols and col not in tail_cols]
    return df[first_cols + remaining + tail_cols]


def _filter_detail_df(
    df: pd.DataFrame,
    keyword: str,
    selected_sections: list[str],
    selected_statuses: list[str],
) -> pd.DataFrame:
    filtered = df.copy()

    if selected_sections and "section" in filtered.columns:
        filtered = filtered[filtered["section"].astype(str).isin(selected_sections)]

    if selected_statuses and "normalized_status" in filtered.columns:
        filtered = filtered[filtered["normalized_status"].astype(str).isin(selected_statuses)]

    keyword = keyword.strip().lower()
    if keyword:
        search_cols = [
            col
            for col in ["Command", "Operator", "Stage", "mongo_short_description"]
            if col in filtered.columns
        ]
        if "mongo_doc_url" in filtered.columns and "mongo_doc_url" not in search_cols:
            search_cols.append("mongo_doc_url")
        if search_cols:
            mask = filtered[search_cols].fillna("").astype(str).apply(
                lambda row: row.str.lower().str.contains(keyword, regex=False).any(),
                axis=1,
            )
            filtered = filtered[mask]

    return filtered


def _filter_usage_detail_df(
    df: pd.DataFrame,
    selected_statuses: list[str],
    selected_op_types: list[str],
    selected_feature_types: list[str],
    selected_commands: list[str],
    selected_complexities: list[str],
    selected_scopes: list[str],
    keyword: str,
) -> pd.DataFrame:
    filtered = df.copy()
    necessity_column = (
        "effective_migration_necessity"
        if "effective_migration_necessity" in filtered.columns
        else "effective_scope"
    )

    if selected_statuses:
        filtered = filtered[filtered["oracle_support_status"].isin(selected_statuses)]
    if selected_op_types and "op_type" in filtered.columns:
        filtered = filtered[filtered["op_type"].isin(selected_op_types)]
    if selected_feature_types:
        filtered = filtered[filtered["feature_type"].isin(selected_feature_types)]
    if selected_commands:
        filtered = filtered[filtered["command_name"].isin(selected_commands)]
    if selected_complexities and "effective_complexity" in filtered.columns:
        filtered = filtered[filtered["effective_complexity"].isin(selected_complexities)]
    if selected_scopes and necessity_column in filtered.columns:
        filtered = filtered[filtered[necessity_column].isin(selected_scopes)]
    if keyword.strip():
        lowered_keyword = keyword.strip().lower()
        search_cols = [
            "feature_name",
            "collection",
            "op_type",
            "command_name",
            "oracle_category",
            "oracle_support_since",
            "oracle_support_status",
            "effective_complexity",
            necessity_column,
            "recommended_action",
        ]
        available_search_cols = [col for col in search_cols if col in filtered.columns]
        if available_search_cols:
            mask = filtered[available_search_cols].fillna("").astype(str).apply(
                lambda row: row.str.lower().str.contains(lowered_keyword, regex=False).any(),
                axis=1,
            )
            filtered = filtered[mask]

    return filtered


def _filter_baseline_df(
    df: pd.DataFrame,
    selected_statuses: list[str],
    selected_feature_types: list[str],
    selected_commands: list[str],
    selected_complexities: list[str],
    selected_scopes: list[str],
    keyword: str,
    only_observed: bool = False,
) -> pd.DataFrame:
    filtered = df.copy()
    necessity_column = (
        "effective_migration_necessity"
        if "effective_migration_necessity" in filtered.columns
        else "effective_scope"
    )

    if only_observed and "observed_in_profile" in filtered.columns:
        filtered = filtered[filtered["observed_in_profile"].fillna(False).astype(bool)]
    if selected_feature_types and "feature_type" in filtered.columns:
        filtered = filtered[filtered["feature_type"].isin(selected_feature_types)]
    if selected_complexities and "effective_complexity" in filtered.columns:
        filtered = filtered[filtered["effective_complexity"].isin(selected_complexities)]
    if selected_scopes and necessity_column in filtered.columns:
        filtered = filtered[filtered[necessity_column].isin(selected_scopes)]
    if selected_statuses and "oracle_support_statuses" in filtered.columns:
        filtered = filtered[
            filtered["oracle_support_statuses"].fillna("").astype(str).apply(
                lambda value: any(status in {item.strip() for item in value.split(",")} for status in selected_statuses)
            )
        ]
    if selected_commands:
        candidate_cols = [col for col in ["feature_name", "observed_command_contexts"] if col in filtered.columns]
        if candidate_cols:
            filtered = filtered[
                filtered[candidate_cols].fillna("").astype(str).apply(
                    lambda row: any(command in {item.strip() for item in ",".join(row.tolist()).split(",")} for command in selected_commands),
                    axis=1,
                )
            ]
    if keyword.strip():
        lowered_keyword = keyword.strip().lower()
        search_cols = [
            col for col in [
                "feature_name",
                "feature_type",
                "oracle_section",
                "oracle_support_since",
                "oracle_support_statuses",
                "mongo_short_description",
                "effective_complexity",
                necessity_column,
                "complexity_explanation",
                "observed_command_contexts",
            ] if col in filtered.columns
        ]
        if search_cols:
            mask = filtered[search_cols].fillna("").astype(str).apply(
                lambda row: row.str.lower().str.contains(lowered_keyword, regex=False).any(),
                axis=1,
            )
            filtered = filtered[mask]

    return filtered


def _unique_api_count(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    key_columns = [
        column
        for column in ["feature_type", "feature_name", "oracle_section", "oracle_category", "oracle_support_since"]
        if column in df.columns
    ]
    if not key_columns:
        return len(df)

    normalized = df[key_columns].copy().fillna("").astype(str)
    if "oracle_section" in normalized.columns and "oracle_category" in normalized.columns:
        normalized["oracle_scope_key"] = normalized["oracle_section"].where(
            normalized["oracle_section"].str.strip().ne(""),
            normalized["oracle_category"],
        )
        normalized = normalized.drop(columns=["oracle_section", "oracle_category"])
    elif "oracle_section" in normalized.columns:
        normalized = normalized.rename(columns={"oracle_section": "oracle_scope_key"})
    elif "oracle_category" in normalized.columns:
        normalized = normalized.rename(columns={"oracle_category": "oracle_scope_key"})

    return len(normalized.drop_duplicates())


def _current_feature_detail_filters(df: pd.DataFrame) -> tuple[str, list[str], list[str]]:
    keyword = str(st.session_state.get("detail_keyword", "") or "")
    raw_sections = st.session_state.get("detail_sections", [])
    raw_statuses = st.session_state.get("detail_statuses", [])

    available_sections = (
        set(df["section"].dropna().astype(str).tolist())
        if "section" in df.columns
        else set()
    )
    available_statuses = (
        set(df["normalized_status"].dropna().astype(str).tolist())
        if "normalized_status" in df.columns
        else set()
    )

    selected_sections = [
        str(value) for value in raw_sections
        if str(value) in available_sections
    ]
    selected_statuses = [
        str(value) for value in raw_statuses
        if str(value) in available_statuses
    ]
    return keyword, selected_sections, selected_statuses


def _prepare_status_summary_display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    display_df = df.copy()
    return display_df.rename(
        columns={
            "normalized_status": "是否支持",
            "count": "数量",
            "percentage": "占比(%)",
        }
    )


def _selected_dataframe_rows(selection_event: object) -> list[int]:
    if selection_event is None:
        return []

    selection = getattr(selection_event, "selection", None)
    if selection is not None:
        rows = getattr(selection, "rows", None)
        if rows is not None:
            return [int(row) for row in rows]

    if isinstance(selection_event, dict):
        selection = selection_event.get("selection", selection_event)
        rows = selection.get("rows", []) if isinstance(selection, dict) else []
        return [int(row) for row in rows]

    return []


def _usage_column_config() -> dict[str, object]:
    return {
        "feature_type": st.column_config.TextColumn(
            "feature_type",
            help="MongoDB 能力类别，例如 command、stage、operator、expression。",
        ),
        "feature_name": st.column_config.TextColumn(
            "feature_name",
            help="具体使用到的 MongoDB 能力项，例如 find、$lookup、$elemMatch、$sum。",
        ),
        "command_name": st.column_config.TextColumn(
            "command_name",
            help="该能力出现在哪个 MongoDB 命令上下文里，例如 aggregate、find、update。",
        ),
        "op_type": st.column_config.TextColumn(
            "op_type",
            help="system.profile.op 的操作类型，例如 command、query、update、insert、remove。",
        ),
        "effective_migration_necessity": st.column_config.TextColumn(
            "迁移必要性",
            help="从应用兼容角度看该 API 是否有必要纳入迁移范围：Required、Conditional、Unnecessary。",
        ),
        "effective_complexity": st.column_config.TextColumn(
            "迁移复杂度",
            help="迁移复杂度分档：Ignore、Low、Medium、High、Blocker。",
        ),
        "migration_priority": st.column_config.TextColumn(
            "迁移优先级",
            help="迁移优先级，独立于复杂度，用于排序热点项。",
        ),
        "recommended_action": st.column_config.TextColumn(
            "建议动作",
            help="针对该 API 的建议动作标签。",
        ),
    }


def _baseline_display_columns(show_advanced: bool) -> list[str]:
    core_columns = [
        "feature_type",
        "feature_name",
        "effective_migration_necessity",
        "oracle_support_since",
        "effective_complexity",
        "complexity_explanation",
        "override_complexity",
        "override_reason",
    ]
    advanced_columns = [
        "oracle_section",
        "oracle_support_statuses",
        "mongo_short_description",
        "observed_in_profile",
        "observed_usage_count",
        "observed_command_contexts",
        "override_enabled",
    ]
    return core_columns + advanced_columns if show_advanced else core_columns


def _safe_feature_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _build_catalog_usage_df(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(
            columns=[
                "feature_type",
                "feature_name",
                "command_name",
                "op_type",
                "database",
                "collection",
                "usage_count",
                "first_seen",
                "last_seen",
                "max_duration_ms",
                "sample_path",
                "sample_value",
                "oracle_support_status",
                "oracle_support_since",
                "oracle_category",
                "mongo_short_description",
            ]
        )

    rows: list[dict[str, object]] = []
    for _, row in detail_df.iterrows():
        command_name = _safe_feature_text(row.get("Command", ""))
        stage_name = _safe_feature_text(row.get("Stage", ""))
        operator_name = _safe_feature_text(row.get("Operator", ""))
        feature_specs: list[tuple[str, str, str]] = []
        if command_name:
            feature_specs.append(("command", command_name, command_name))
        if stage_name:
            feature_specs.append(("stage", stage_name, "aggregate"))
        if operator_name:
            feature_specs.append(("operator", operator_name, command_name))

        for feature_type, feature_name, command_context in feature_specs:
            rows.append(
                {
                    "feature_type": feature_type,
                    "feature_name": feature_name,
                    "command_name": command_context,
                    "op_type": "",
                    "database": "",
                    "collection": "",
                    "usage_count": 1,
                    "first_seen": "",
                    "last_seen": "",
                    "max_duration_ms": None,
                    "sample_path": "",
                    "sample_value": "",
                    "oracle_support_status": str(
                        row.get("normalized_status", "") or row.get("oracle_support_status", "") or "Unknown"
                    ),
                    "oracle_support_since": str(row.get("Support (Since)", "") or ""),
                    "oracle_category": str(row.get("section", "") or ""),
                    "mongo_short_description": str(row.get("mongo_short_description", "") or ""),
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _format_report_table(df: pd.DataFrame, link_columns: set[str] | None = None) -> str:
    if df.empty:
        return "<p class='empty-state'>暂无数据</p>"

    link_columns = link_columns or set()
    formatted = df.copy().fillna("")
    for col in formatted.columns:
        if col in link_columns:
            formatted[col] = formatted[col].map(
                lambda value: (
                    f"<a href=\"{html_lib.escape(str(value))}\" target=\"_blank\">打开链接</a>"
                    if str(value).strip()
                    else ""
                )
            )
        else:
            formatted[col] = formatted[col].map(lambda value: html_lib.escape(str(value)))

    return formatted.to_html(index=False, escape=False, classes="report-table")


def _mongodb_source_pages_html(metadata: dict[str, object]) -> str:
    pages = _displayable_mongodb_source_pages(
        metadata,
        str(metadata.get("mongodb_manual_about_url", "")),
    )
    if not pages:
        return "<p class='empty-state'>除当前 MongoDB 版本页外，暂无其他 MongoDB 文档源链接。</p>"

    items: list[str] = []
    for page in pages:
        label = html_lib.escape(str(page.get("label", "")))
        url = html_lib.escape(str(page.get("url", "")))
        entry_count = page.get("entry_count", "")
        count_text = ""
        if str(entry_count).strip() != "":
            count_text = f"<span class='source-pill'>{html_lib.escape(str(entry_count))} 条</span>"
        items.append(
            f"""
            <div class="source-link-item">
              <div class="source-link-header">
                <span class="source-link-label">{label}</span>
                {count_text}
              </div>
              <div class="source-link-url"><a href="{url}" target="_blank" rel="noreferrer">{url}</a></div>
            </div>
            """
        )
    return "<div class='source-link-list'>" + "".join(items) + "</div>"


def _prepare_detail_display_df(
    df: pd.DataFrame,
    include_section: bool = False,
    section_name: str | None = None,
) -> pd.DataFrame:
    prepared = df.copy()
    if not include_section:
        prepared = prepared.drop(columns=["section"], errors="ignore")
    prepared = prepared.drop(columns=["table_index"], errors="ignore")
    prepared = prepared.drop(columns=["mongo_entity_type"], errors="ignore")
    prepared = prepared.drop(columns=["mongo_source_group"], errors="ignore")
    prepared = prepared.drop(columns=["mongo_name"], errors="ignore")
    prepared = prepared.drop(columns=["mongo_reference_category"], errors="ignore")
    prepared = prepared.drop(columns=["mongo_last_synced_at"], errors="ignore")
    prepared = _drop_empty_rows(prepared)
    prepared = _drop_empty_columns(prepared)
    prepared = _reorder_detail_columns(prepared, section_name=section_name)
    prepared = prepared.rename(
        columns={
            "normalized_status": "支持判断",
            "mongo_short_description": "功能说明",
            "mongo_doc_url": "MongoDB 官方文档",
        }
    )
    return prepared


def _json_for_html(value: object) -> str:
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def _build_offline_report_html(
    output_dir: str,
    doc_metadata: dict[str, str],
    reference_metadata: dict[str, str | int],
    status_summary_df: pd.DataFrame,
    support_top_display_df: pd.DataFrame,
    section_display_df: pd.DataFrame,
    filtered_detail_df: pd.DataFrame,
    oracle_target_version: str,
    oracle_target_mode: str,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    oracle_target_mode_display = _oracle_target_mode_label(oracle_target_mode)
    detail_display_df = _prepare_detail_display_df(filtered_detail_df, include_section=True)
    detail_columns = [col for col in detail_display_df.columns if col != "section"]
    oracle_doc_source_url = str(doc_metadata.get("doc_source_url", "") or INDEX_URL)
    mongodb_manual_about_url = str(reference_metadata.get("mongodb_manual_about_url", "") or ABOUT_URL)
    offline_doc_link_urls = {
        definition["id"]: definition["default_url"] for definition in _document_link_definitions()
    }
    offline_doc_link_urls["oracle_feature_support"] = TARGET_URL
    offline_doc_link_urls["oracle_version_source"] = oracle_doc_source_url
    offline_doc_link_urls["manual_version"] = mongodb_manual_about_url
    for page in _mongodb_source_pages(reference_metadata):
        kind = str(page.get("kind", "")).strip()
        url = str(page.get("url", "")).strip()
        if kind and url:
            offline_doc_link_urls[kind] = url
    document_link_df = pd.DataFrame(
        _document_link_rows(
            reference_metadata,
            offline_doc_link_urls,
            oracle_entry_count=len(filtered_detail_df),
        )
    )
    if not document_link_df.empty:
        document_link_df["上次抓取数"] = document_link_df["上次抓取数"].map(
            lambda value: str(int(value)) if str(value).strip() not in {"", "None"} else "-"
        )
        document_link_df = document_link_df[["文档项", "链接", "上次抓取数", "用途说明"]]
    detail_rows: list[dict[str, str]] = []
    for _, row in detail_display_df.fillna("").iterrows():
        record = {col: str(row.get(col, "") or "") for col in detail_columns}
        record["__section"] = str(row.get("section", "") or "")
        record["__support_since"] = str(row.get("Support (Since)", "") or "")
        record["__status"] = str(filtered_detail_df.loc[row.name, "normalized_status"] or "") if "normalized_status" in filtered_detail_df.columns else ""
        detail_rows.append(record)

    payload = {
        "generatedAt": generated_at,
        "outputDir": output_dir,
        "detailCount": len(filtered_detail_df),
        "oracleDocVersionDate": doc_metadata.get("doc_version_date", "") or "未解析到",
        "oracleDocId": doc_metadata.get("doc_id", "") or "未解析到",
        "oracleDocSourceUrl": oracle_doc_source_url,
        "oracleUpdateStatus": doc_metadata.get("update_status", ""),
        "mongodbManualVersion": str(reference_metadata.get("mongodb_manual_version", "")) or "未同步",
        "mongodbManualAboutUrl": mongodb_manual_about_url,
        "mongodbSyncedAt": str(reference_metadata.get("synced_at", "未知")),
        "mongodbNewEntryCount": int(reference_metadata.get("new_entry_count", 0) or 0),
        "mongodbUpdatedEntryCount": int(reference_metadata.get("updated_entry_count", 0) or 0),
        "mongodbDescriptionCoverage": int(
            filtered_detail_df["mongo_short_description"].fillna("").astype(str).str.strip().ne("").sum()
        ) if "mongo_short_description" in filtered_detail_df.columns else 0,
        "oracleTargetVersion": oracle_target_version,
        "oracleTargetMode": oracle_target_mode_display,
        "detailColumns": detail_columns,
        "detailRows": detail_rows,
        "documentLinks": document_link_df.fillna("").astype(str).to_dict(orient="records"),
        "summaryTables": {
            "status": _prepare_status_summary_display_df(status_summary_df)
            .fillna("")
            .astype(str)
            .to_dict(orient="records"),
            "support": support_top_display_df.fillna("").astype(str).to_dict(orient="records"),
            "section": section_display_df.fillna("").astype(str).to_dict(orient="records"),
        },
    }

    return f"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Oracle Database API for MongoDB Feature Support</title>
  <style>
    :root {{
      --bg: #f4f7f8;
      --panel: #ffffff;
      --panel-soft: #f8fbfb;
      --line: #dde5e7;
      --text: #15202b;
      --muted: #57727a;
      --brand-1: #0f3b45;
      --brand-2: #176b72;
      --brand-3: #2f8f83;
      --ok: #2e8b57;
      --warn: #f0ad4e;
      --bad: #d9534f;
      --other: #6c757d;
      --shadow: 0 10px 28px rgba(12, 52, 59, 0.06);
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      background:
        radial-gradient(circle at top left, rgba(47, 143, 131, 0.08), transparent 28%),
        radial-gradient(circle at top right, rgba(15, 59, 69, 0.08), transparent 26%),
        var(--bg);
      color: var(--text);
    }}
    .page {{
      max-width: min(96vw, 1600px);
      margin: 0 auto;
      padding: 24px;
    }}
    .hero {{
      background: linear-gradient(135deg, #0f3b45 0%, #176b72 56%, #2f8f83 100%);
      color: #fff;
      border-radius: 8px;
      padding: 24px;
      margin-bottom: 20px;
      box-shadow: var(--shadow);
    }}
    .hero h1 {{
      margin: 0 0 8px 0;
      font-size: 28px;
    }}
    .meta-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}
    .meta-card, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      box-shadow: var(--shadow);
    }}
    .meta-card .label {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 6px;
    }}
    .meta-card .value {{
      font-size: 22px;
      font-weight: 700;
    }}
    .panel {{
      margin-bottom: 18px;
    }}
    .panel h2 {{
      margin: 0 0 12px 0;
      font-size: 20px;
    }}
    .panel-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
    }}
    .filter-grid {{
      display: grid;
      grid-template-columns: 1.3fr 1fr 1fr 0.8fr;
      gap: 12px;
      margin-bottom: 16px;
    }}
    .filter-control label {{
      display: block;
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 6px;
      font-weight: 600;
    }}
    .text-input {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel-soft);
      color: var(--text);
      padding: 10px 12px;
      font-size: 14px;
    }}
    .multi-select {{
      position: relative;
    }}
    .multi-select-button {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel-soft);
      color: var(--text);
      padding: 10px 12px;
      font-size: 14px;
      text-align: left;
      cursor: pointer;
    }}
    .multi-select-button::after {{
      content: "▾";
      float: right;
      color: var(--muted);
    }}
    .multi-select.open .multi-select-button::after {{
      content: "▴";
    }}
    .multi-select-menu {{
      display: none;
      position: absolute;
      left: 0;
      right: 0;
      top: calc(100% + 6px);
      z-index: 20;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      max-height: 260px;
      overflow: auto;
      padding: 8px;
    }}
    .multi-select.open .multi-select-menu {{
      display: block;
    }}
    .multi-select-option {{
      display: flex;
      align-items: flex-start;
      gap: 8px;
      padding: 8px 6px;
      border-radius: 6px;
      font-size: 14px;
      cursor: pointer;
    }}
    .multi-select-option:hover {{
      background: #eef5f5;
    }}
    .multi-select-option input {{
      margin-top: 2px;
    }}
    .multi-select-empty {{
      color: var(--muted);
      font-size: 13px;
      padding: 8px 6px;
    }}
    .checkbox-row {{
      display: flex;
      align-items: center;
      gap: 8px;
      min-height: 42px;
      margin-top: 26px;
    }}
    .toolbar {{
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 16px;
      flex-wrap: wrap;
    }}
    .btn {{
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--panel-soft);
      color: var(--text);
      padding: 8px 14px;
      font-size: 13px;
      cursor: pointer;
    }}
    .btn:hover {{
      border-color: var(--brand-2);
    }}
    .chart-block {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel-soft);
      padding: 14px;
      margin-bottom: 14px;
    }}
    .chart-block h3 {{
      margin: 0 0 10px 0;
      font-size: 16px;
    }}
    .chart-svg {{
      width: 100%;
      height: auto;
      display: block;
    }}
    .chart-legend {{
      display: grid;
      gap: 8px;
      margin-top: 12px;
    }}
    .chart-legend-item {{
      display: grid;
      grid-template-columns: 12px minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
      font-size: 13px;
      color: var(--text);
    }}
    .chart-legend-swatch {{
      width: 12px;
      height: 12px;
      border-radius: 999px;
      display: inline-block;
    }}
    .chart-legend-value {{
      color: var(--muted);
      font-variant-numeric: tabular-nums;
    }}
    .bars {{
      display: grid;
      gap: 10px;
    }}
    .bar-row {{
      display: grid;
      grid-template-columns: minmax(140px, 220px) minmax(0, 1fr) 70px;
      gap: 10px;
      align-items: center;
      font-size: 13px;
    }}
    .bar-label {{
      color: var(--text);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .bar-track {{
      width: 100%;
      background: #eaf0f1;
      border-radius: 999px;
      overflow: hidden;
      height: 12px;
    }}
    .bar-fill {{
      height: 100%;
      border-radius: 999px;
      min-width: 2px;
    }}
    .bar-value {{
      text-align: right;
      color: var(--muted);
      font-variant-numeric: tabular-nums;
    }}
    .report-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    .report-table th, .report-table td {{
      border: 1px solid var(--line);
      padding: 8px 10px;
      vertical-align: top;
      text-align: left;
    }}
    .report-table th {{
      background: #f1f5f6;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    .section-group {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel-soft);
      margin-bottom: 12px;
      overflow: hidden;
    }}
    .section-group summary {{
      cursor: pointer;
      list-style: none;
      padding: 14px 16px;
      font-weight: 700;
      display: grid;
      grid-template-columns: minmax(220px, 320px) minmax(180px, 1fr) 86px;
      align-items: center;
      gap: 12px;
    }}
    .section-group summary::-webkit-details-marker {{
      display: none;
    }}
    .section-count {{
      color: var(--muted);
      font-weight: 600;
      font-size: 13px;
      text-align: right;
    }}
    .section-summary-bar {{
      display: flex;
      width: 100%;
      height: 12px;
      border-radius: 999px;
      background: #e7eef0;
      overflow: hidden;
    }}
    .section-summary-bar-fill {{
      display: block;
      flex: 0 0 auto;
      height: 100%;
      min-width: 2px;
    }}
    .section-body {{
      padding: 0 16px 16px 16px;
    }}
    .note {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }}
    .empty-state {{
      color: var(--muted);
      margin: 0;
    }}
    .version-lines {{
      display: grid;
      gap: 6px;
    }}
    .source-links-wrap {{
      margin-top: 12px;
    }}
    .source-links-wrap details {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel-soft);
      padding: 10px 12px;
    }}
    .source-links-wrap summary {{
      cursor: pointer;
      color: var(--text);
      font-weight: 600;
    }}
    .source-link-list {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-top: 10px;
    }}
    .source-link-item {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      padding: 10px 12px;
    }}
    .source-link-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 6px;
    }}
    .source-link-label {{
      font-size: 13px;
      font-weight: 700;
    }}
    .source-link-url {{
      font-size: 12px;
      line-height: 1.45;
      word-break: break-all;
    }}
    .source-pill {{
      display: inline-block;
      font-size: 11px;
      color: var(--muted);
      background: #edf4f4;
      border-radius: 999px;
      padding: 2px 8px;
      white-space: nowrap;
    }}
    a {{
      color: var(--brand-2);
      text-decoration: none;
    }}
    a:hover {{
      text-decoration: underline;
    }}
    @media (max-width: 960px) {{
      .meta-grid, .panel-grid, .filter-grid {{
        grid-template-columns: 1fr;
      }}
      .bar-row, .section-group summary, .source-link-list {{
        grid-template-columns: 1fr;
      }}
      .section-count {{
        text-align: left;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="hero">
      <h1>Oracle Database API for MongoDB Feature Support</h1>
      <div>离线报告</div>
      <div class="note">生成时间：{generated_at}</div>
    </div>

    <section class="panel">
      <h2>结果概览</h2>
      <div class="meta-grid">
        <div class="meta-card"><div class="label">明细记录数</div><div class="value">{len(filtered_detail_df)}</div></div>
        <div class="meta-card"><div class="label">Oracle 文档版本时间</div><div class="value">{html_lib.escape(doc_metadata.get('doc_version_date', '') or '未解析到')}</div></div>
        <div class="meta-card"><div class="label">Oracle 文档编号</div><div class="value">{html_lib.escape(doc_metadata.get('doc_id', '') or '未解析到')}</div></div>
        <div class="meta-card"><div class="label">MongoDB 手册版本</div><div class="value">{html_lib.escape(str(reference_metadata.get('mongodb_manual_version', '')) or '未同步')}</div></div>
      </div>
      <div class="version-lines">
        <div class="note">导出时间：{generated_at}</div>
        <div class="note">结果目录：{html_lib.escape(output_dir)}</div>
        <div class="note">当前支持判断：Oracle {html_lib.escape(oracle_target_version)} + {html_lib.escape(oracle_target_mode_display)}</div>
        <div class="note">MongoDB 说明同步：{html_lib.escape(str(reference_metadata.get('synced_at', '未知')))} | 新增 {html_lib.escape(str(reference_metadata.get('new_entry_count', 0)))} | 更新 {html_lib.escape(str(reference_metadata.get('updated_entry_count', 0)))} | 已覆盖 {html_lib.escape(str(payload["mongodbDescriptionCoverage"]))}</div>
        <div class="note">Oracle 版本状态：{html_lib.escape(doc_metadata.get('update_status', ''))}</div>
      </div>
    </section>

    <section class="panel">
      <h2>文档链接与来源</h2>
      <div class="table-wrap">
        {_format_report_table(document_link_df, link_columns={"链接"}) if not document_link_df.empty else "<p class='empty-state'>暂无文档链接。</p>"}
      </div>
    </section>

    <div class="panel-grid">
      <section class="panel">
        <h2>支持情况分析</h2>
        <div id="status-context-note" class="note"></div>
        <div id="status-chart" class="chart-block"></div>
      </section>
      <section class="panel">
      <h2>模式与版本演进</h2>
        <div id="support-chart" class="chart-block"></div>
      </section>
    </div>

    <section class="panel">
      <h2>Feature Support 明细</h2>
      <div class="filter-grid">
        <div class="filter-control">
          <label for="oracle-version-filter">目标 Oracle 数据库版本</label>
          <select id="oracle-version-filter" class="text-input" disabled></select>
        </div>
        <div class="filter-control">
          <label for="oracle-mode-filter">部署方式</label>
          <select id="oracle-mode-filter" class="text-input" disabled></select>
        </div>
        <div class="filter-control" style="grid-column: span 2;">
          <label>支持判断说明</label>
          <div class="note">按所选 Oracle 版本和部署方式组合，动态重算当前离线报告中的支持状态。</div>
        </div>
      </div>
      <div class="filter-grid">
        <div class="filter-control">
          <label for="keyword-filter">关键字搜索</label>
          <input id="keyword-filter" class="text-input" type="text" placeholder="搜索 Command / Operator / Stage / 功能说明" />
        </div>
        <div class="filter-control">
          <label>按 section 筛选</label>
          <div id="section-filter" class="multi-select"></div>
        </div>
        <div class="filter-control">
          <label>按当前组合支持判断筛选</label>
          <div id="status-filter" class="multi-select"></div>
        </div>
        <div class="filter-control">
          <div class="checkbox-row">
            <input id="desc-only-filter" type="checkbox" />
            <label for="desc-only-filter">仅看有说明</label>
          </div>
        </div>
      </div>
      <div class="toolbar">
        <button id="clear-filters" class="btn" type="button">清空筛选</button>
        <button id="expand-all" class="btn" type="button">展开全部 section</button>
        <button id="collapse-all" class="btn" type="button">折叠全部 section</button>
      </div>
      <div id="detail-sections"></div>
    </section>
  </div>
  <script>
    const reportData = {_json_for_html(payload)};

    const statusColorMap = {{
      "Supported": "#2e8b57",
      "Not Supported": "#d9534f",
      "Partially Supported": "#f0ad4e",
      "Other": "#6c757d",
    }};
    const palette = ["#0f3b45", "#176b72", "#2f8f83", "#58a69b", "#7cc3b5", "#f0ad4e", "#e67e22", "#4c78a8", "#72b7b2", "#54a24b", "#9d755d", "#b279a2"];

    const keywordFilter = document.getElementById("keyword-filter");
    const oracleVersionFilter = document.getElementById("oracle-version-filter");
    const oracleModeFilter = document.getElementById("oracle-mode-filter");
    const sectionFilter = document.getElementById("section-filter");
    const statusFilter = document.getElementById("status-filter");
    const descOnlyFilter = document.getElementById("desc-only-filter");
      const clearFiltersButton = document.getElementById("clear-filters");
      const expandAllButton = document.getElementById("expand-all");
      const collapseAllButton = document.getElementById("collapse-all");

    function escapeHtml(value) {{
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }}

    function toNumber(value) {{
      const number = Number(value);
      return Number.isFinite(number) ? number : 0;
    }}

    function uniqueSorted(values) {{
      return [...new Set(values.filter((value) => String(value || "").trim() !== ""))]
        .sort((left, right) => String(left).localeCompare(String(right), "zh-CN"));
    }}

    function selectedValues(container) {{
      return Array.from(container.querySelectorAll("input[type='checkbox']:checked")).map((input) => input.value);
    }}

    function normalizeOracleVersion(value) {{
      return String(value || "").trim().toLowerCase().replaceAll(" ", "");
    }}

    function normalizeSupportMode(value) {{
      const normalized = String(value || "").trim().toLowerCase();
      if (["any", "任意部署方式", "任意"].includes(normalized)) {{
        return "any";
      }}
      if (["noop", "no-op", "no_op"].includes(normalized)) {{
        return "no-op";
      }}
      return "op";
    }}

    function oracleVersionRank(value) {{
      const normalized = normalizeOracleVersion(value);
      const match = normalized.match(/^(\\d+)(ai|c)$/);
      if (!match) {{
        return null;
      }}
      return [Number(match[1]), match[2] === "ai" ? 1 : 0];
    }}

    function compareRanks(left, right) {{
      if (!left || !right) {{
        return 0;
      }}
      if (left[0] !== right[0]) {{
        return left[0] - right[0];
      }}
      return left[1] - right[1];
    }}

    function extractOracleVersions(rawValue) {{
      const matches = String(rawValue || "").match(/\\b(\\d+\\s*(?:ai|c))\\b/gi) || [];
      const values = [];
      matches.forEach((token) => {{
        const normalized = normalizeOracleVersion(token);
        if (normalized && !values.includes(normalized)) {{
          values.push(normalized);
        }}
      }});
      return values;
    }}

    function availableOracleVersions() {{
      const values = [];
      reportData.detailRows.forEach((row) => {{
        extractOracleVersions(row.__support_since || "").forEach((version) => {{
          if (!values.includes(version)) {{
            values.push(version);
          }}
        }});
      }});
      if (!values.length) {{
        values.push("19c", "26ai");
      }}
      values.sort((left, right) => compareRanks(oracleVersionRank(left), oracleVersionRank(right)));
      return values;
    }}

    function effectiveOracleSupportStatus(supportSinceValue, targetVersion, targetMode) {{
      const text = String(supportSinceValue || "").trim();
      if (!text) {{
        return "Unknown";
      }}

      const low = text.toLowerCase();
      if (["not supported", "unsupported", "not applicable"].some((item) => low.includes(item)) || ["no", "n/a", "na"].includes(low)) {{
        return "Not Supported";
      }}

      const candidateVersions = extractOracleVersions(text);
      const versionsToCheck = ["", "任意版本", "任意"].includes(String(targetVersion || "").trim())
        ? (candidateVersions.length ? candidateVersions : ["19c", "26ai"])
        : [normalizeOracleVersion(targetVersion)];
      const normalizedTargetMode = normalizeSupportMode(targetMode);
      const modesToCheck = normalizedTargetMode === "any" ? ["op", "no-op"] : [normalizedTargetMode];

      const noOpOnlyVersions = [];
      if (low.includes("no-op")) {{
        const matches = String(text).match(/\\(([^)]*)\\)/g) || [];
        matches.forEach((segment) => {{
          extractOracleVersions(segment).forEach((version) => {{
            if (!noOpOnlyVersions.includes(version)) {{
              noOpOnlyVersions.push(version);
            }}
          }});
        }});
      }}

      const baseText = text.replace(/\\([^)]*\\)/g, " ");
      const baseVersions = extractOracleVersions(baseText);

      for (const version of versionsToCheck) {{
        const versionRank = oracleVersionRank(version);
        for (const mode of modesToCheck) {{
          if (low === "no-op") {{
            if (mode === "no-op" && ["19c", "26ai"].includes(version)) {{
              return "Supported";
            }}
            continue;
          }}

          if (mode === "no-op" && noOpOnlyVersions.includes(version)) {{
            return "Supported";
          }}

          if (baseVersions.length && versionRank) {{
            const rankedVersions = baseVersions
              .map((item) => oracleVersionRank(item))
              .filter(Boolean)
              .sort(compareRanks);
            if (rankedVersions.length && compareRanks(versionRank, rankedVersions[0]) >= 0) {{
              return "Supported";
            }}
          }}
        }}
      }}

      return "Not Supported";
    }}

    function currentOracleTargetVersion() {{
      return oracleVersionFilter.value || reportData.oracleTargetVersion || "任意版本";
    }}

    function currentOracleTargetMode() {{
      return normalizeSupportMode(
        oracleModeFilter.value || reportData.oracleTargetMode || "任意部署方式"
      ) === "any"
        ? "任意部署方式"
        : normalizeSupportMode(
            oracleModeFilter.value || reportData.oracleTargetMode || "任意部署方式"
          );
    }}

    function withEffectiveStatus(row) {{
      const effectiveStatus = effectiveOracleSupportStatus(
        row.__support_since || "",
        currentOracleTargetVersion(),
        currentOracleTargetMode(),
      );
      return {{
        ...row,
        "支持判断": effectiveStatus,
        "__status": effectiveStatus,
      }};
    }}

    function reorderDetailColumns(columns, sectionName) {{
      const sectionText = String(sectionName || "").trim().toLowerCase();
      let sectionSpecificFront = [];
      if (sectionText.includes("data types")) {{
        sectionSpecificFront = ["Data Types", "Data Type", "Data Type and Alias", "BSON Type", "Alias"];
      }} else if (sectionText.includes("index options")) {{
        sectionSpecificFront = ["Index Option", "Index option"];
      }} else if (sectionText.includes("index")) {{
        sectionSpecificFront = ["Index Type", "Index type"];
      }}

      const genericFront = [
        "Command",
        "Operator",
        "Stage",
        "Feature",
        "支持判断",
        "Data Type and Alias",
        "Data Type",
        "Index Type",
        "Index Option",
        "Field",
        "Fields",
        "Expression",
        "Accumulator",
        "Option",
        "Options",
        "Type",
        "Name",
        "Support (Since)",
      ];
      const genericTail = ["功能说明", "MongoDB 官方文档", "Notes", "Note"];

      const pickExisting = (candidates) => {{
        const picked = [];
        candidates.forEach((candidate) => {{
          columns.forEach((column) => {{
            if (column === candidate && !picked.includes(column)) {{
              picked.push(column);
            }}
          }});
        }});
        return picked;
      }};

      const front = pickExisting(sectionSpecificFront.concat(genericFront));
      const tail = pickExisting(genericTail);
      const middle = columns.filter((column) => !front.includes(column) && !tail.includes(column));
      return front.concat(middle, tail);
    }}

    function percentage(count, total) {{
      if (!total) {{
        return "0.00";
      }}
      return ((count / total) * 100).toFixed(2);
    }}

    function aggregateCounts(rows, keyName, fallbackLabel) {{
      const counter = new Map();
      rows.forEach((row) => {{
        const label = String(row[keyName] || "").trim() || fallbackLabel;
        counter.set(label, (counter.get(label) || 0) + 1);
      }});
      return Array.from(counter.entries())
        .map(([label, count]) => ({{
          label,
          count,
          percentage: percentage(count, rows.length),
        }}))
        .sort((left, right) => right.count - left.count || left.label.localeCompare(right.label, "zh-CN"));
    }}

    function createTable(columns, rows, linkColumns = []) {{
      if (!rows.length) {{
        return "<p class='empty-state'>暂无数据</p>";
      }}
      const linkSet = new Set(linkColumns);
      const thead = `<thead><tr>${{columns.map((column) => `<th>${{escapeHtml(column)}}</th>`).join("")}}</tr></thead>`;
      const tbody = rows.map((row) => {{
        const cells = columns.map((column) => {{
          const value = row[column] ?? "";
          if (linkSet.has(column) && String(value).trim()) {{
            return `<td><a href="${{escapeHtml(value)}}" target="_blank" rel="noreferrer">打开链接</a></td>`;
          }}
          return `<td>${{escapeHtml(value)}}</td>`;
        }}).join("");
        return `<tr>${{cells}}</tr>`;
      }}).join("");
      return `<table class="report-table">${{thead}}<tbody>${{tbody}}</tbody></table>`;
    }}

    function sectionStatusSegments(rows) {{
      const counts = {{
        "Supported": 0,
        "Not Supported": 0,
        "Partially Supported": 0,
        "Other": 0,
      }};
      rows.forEach((row) => {{
        const status = String(row.__status || "").trim() || "Other";
        if (Object.prototype.hasOwnProperty.call(counts, status)) {{
          counts[status] += 1;
        }} else {{
          counts.Other += 1;
        }}
      }});
      return [
        ["Supported", counts.Supported, statusColorMap.Supported],
        ["Not Supported", counts["Not Supported"], statusColorMap["Not Supported"]],
        ["Partially Supported", counts["Partially Supported"], statusColorMap["Partially Supported"]],
        ["Other", counts.Other, statusColorMap.Other],
      ].filter((item) => item[1] > 0);
    }}

    function sectionStatusBar(rows) {{
      const total = Math.max(rows.length, 1);
      const segments = sectionStatusSegments(rows);
      const html = segments.map(([label, count, color]) => {{
        const width = Math.max((count / total) * 100, 2);
        return `<span class="section-summary-bar-fill" title="${{escapeHtml(label)}}: ${{count}}" style="width:${{width}}%;background:${{color}};"></span>`;
      }}).join("");
      return `<span class="section-summary-bar">${{html}}</span>`;
    }}

    function polarToCartesian(cx, cy, radius, angleInDegrees) {{
      const angleInRadians = ((angleInDegrees - 90) * Math.PI) / 180.0;
      return {{
        x: cx + (radius * Math.cos(angleInRadians)),
        y: cy + (radius * Math.sin(angleInRadians)),
      }};
    }}

    function describeArc(cx, cy, radius, startAngle, endAngle) {{
      const start = polarToCartesian(cx, cy, radius, endAngle);
      const end = polarToCartesian(cx, cy, radius, startAngle);
      const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";
      return [
        "M", start.x, start.y,
        "A", radius, radius, 0, largeArcFlag, 0, end.x, end.y,
      ].join(" ");
    }}

    function renderStatusChart(targetId, title, items, colorResolver) {{
      const target = document.getElementById(targetId);
      if (!items.length) {{
        target.innerHTML = `<h3>${{escapeHtml(title)}}</h3><p class="empty-state">暂无数据</p>`;
        return;
      }}
      const width = 700;
      const height = 240;
      const margin = {{ top: 18, right: 96, bottom: 28, left: 132 }};
      const chartWidth = width - margin.left - margin.right;
      const chartHeight = height - margin.top - margin.bottom;
      const maxCount = Math.max(...items.map((item) => item.count), 1);
      const barGap = 18;
      const barHeight = Math.max((chartHeight - barGap * (items.length - 1)) / Math.max(items.length, 1), 26);
      const yForIndex = (index) => margin.top + index * (barHeight + barGap);
      const xScale = (value) => margin.left + (value / maxCount) * chartWidth;
      const ticks = 5;
      const gridSvg = Array.from({{ length: ticks + 1 }}, (_, index) => {{
        const tickValue = Math.round((maxCount / ticks) * index);
        const x = xScale(tickValue);
        return `
          <line x1="${{x}}" y1="${{margin.top}}" x2="${{x}}" y2="${{margin.top + chartHeight}}" stroke="#d9e4e7" stroke-width="1" />
          <text x="${{x}}" y="${{margin.top + chartHeight + 18}}" text-anchor="middle" font-size="12" fill="#6b7280">${{tickValue}}</text>
        `;
      }}).join("");
      const barsSvg = items.map((item, index) => {{
        const color = colorResolver(item, index);
        const y = yForIndex(index);
        const barWidth = Math.max(xScale(item.count) - margin.left, 2);
        const labelY = y + barHeight / 2 + 4;
        const displayLabel = `${{item.count}} | ${{item.percentage}}%`;
        return `
          <g>
            <text x="${{margin.left - 12}}" y="${{labelY}}" text-anchor="end" font-size="12" font-weight="700" fill="#31434a">${{escapeHtml(item.label)}}</text>
            <rect x="${{margin.left}}" y="${{y}}" width="${{barWidth}}" height="${{barHeight}}" rx="4" fill="${{color}}">
              <title>${{escapeHtml(item.label)}}: ${{item.count}} (${{item.percentage}}%)</title>
            </rect>
            <text x="${{margin.left + barWidth + 8}}" y="${{labelY}}" text-anchor="start" font-size="12" font-weight="700" fill="#31434a">${{escapeHtml(displayLabel)}}</text>
          </g>
        `;
      }}).join("");
      const axisSvg = `
        <line x1="${{margin.left}}" y1="${{margin.top + chartHeight}}" x2="${{margin.left + chartWidth}}" y2="${{margin.top + chartHeight}}" stroke="#cfd8db" stroke-width="1" />
      `;
      target.innerHTML = `
        <h3>${{escapeHtml(title)}}</h3>
        <svg class="chart-svg" viewBox="0 0 ${{width}} ${{height}}" role="img" aria-label="${{escapeHtml(title)}}">
          ${{gridSvg}}
          ${{axisSvg}}
          ${{barsSvg}}
        </svg>
      `;
    }}

    function renderSupportDonut(targetId, title, items, colorResolver) {{
      const target = document.getElementById(targetId);
      if (!items.length) {{
        target.innerHTML = `<h3>${{escapeHtml(title)}}</h3><p class="empty-state">暂无数据</p>`;
        return;
      }}
      const total = items.reduce((sum, item) => sum + item.count, 0);
      const cx = 110;
      const cy = 110;
      const radius = 72;
      const strokeWidth = 34;
      let startAngle = 0;
      const arcsSvg = items.map((item, index) => {{
        const portion = total ? (item.count / total) * 360 : 0;
        const endAngle = startAngle + portion;
        const color = colorResolver(item, index);
        const path = describeArc(cx, cy, radius, startAngle, endAngle);
        startAngle = endAngle;
        return `<path d="${{path}}" fill="none" stroke="${{color}}" stroke-width="${{strokeWidth}}" stroke-linecap="butt"><title>${{escapeHtml(item.label)}}: ${{item.count}} (${{item.percentage}}%)</title></path>`;
      }}).join("");
      const legendHtml = items.map((item, index) => {{
        const color = colorResolver(item, index);
        return `
          <div class="chart-legend-item">
            <span class="chart-legend-swatch" style="background:${{color}};"></span>
            <span>${{escapeHtml(item.label)}}</span>
            <span class="chart-legend-value">${{item.count}} / ${{item.percentage}}%</span>
          </div>
        `;
      }}).join("");
      target.innerHTML = `
        <h3>${{escapeHtml(title)}}</h3>
        <svg class="chart-svg" viewBox="0 0 220 220" role="img" aria-label="${{escapeHtml(title)}}">
          <circle cx="${{cx}}" cy="${{cy}}" r="${{radius}}" fill="none" stroke="#e7eef0" stroke-width="${{strokeWidth}}"></circle>
          ${{arcsSvg}}
          <text x="${{cx}}" y="${{cy - 6}}" text-anchor="middle" font-size="14" fill="#57727a">总计</text>
          <text x="${{cx}}" y="${{cy + 18}}" text-anchor="middle" font-size="20" font-weight="700" fill="#15202b">${{total}}</text>
        </svg>
        <div class="chart-legend">${{legendHtml}}</div>
      `;
    }}

    function renderModeVersionChart(targetId, title, rows) {{
      const target = document.getElementById(targetId);
      if (!rows.length) {{
        target.innerHTML = `<h3>${{escapeHtml(title)}}</h3><p class="empty-state">暂无数据</p>`;
        return;
      }}

      const versions = ["19c", "26ai"];
      const modes = ["op", "no-op"];
      const modeColors = {{
        op: "#176b72",
        "no-op": "#d97706",
      }};
      const width = 760;
      const height = 320;
      const margin = {{ top: 28, right: 32, bottom: 56, left: 64 }};
      const chartWidth = width - margin.left - margin.right;
      const chartHeight = height - margin.top - margin.bottom;
      const maxValue = Math.max(...rows.map((item) => toNumber(item.supported_count)), toNumber(rows[0].total_feature_count), 1);
      const groupWidth = chartWidth / versions.length;
      const barWidth = Math.min(68, groupWidth * 0.22);
      const groupCenter = (index) => margin.left + groupWidth * index + groupWidth / 2;
      const barGap = 14;
      const barX = (versionIndex, modeIndex) => {{
        const groupMid = groupCenter(versionIndex);
        const totalBarsWidth = barWidth * 2 + barGap;
        const groupStart = groupMid - totalBarsWidth / 2;
        return groupStart + modeIndex * (barWidth + barGap);
      }};
      const yScale = (value) => margin.top + chartHeight - (toNumber(value) / maxValue) * chartHeight;

      const ticks = 5;
      const gridSvg = Array.from({{ length: ticks + 1 }}, (_, index) => {{
        const tickValue = Math.round((maxValue / ticks) * index);
        const y = yScale(tickValue);
        return `
          <line x1="${{margin.left}}" y1="${{y}}" x2="${{margin.left + chartWidth}}" y2="${{y}}" stroke="#d9e4e7" stroke-width="1" />
          <text x="${{margin.left - 12}}" y="${{y + 4}}" text-anchor="end" font-size="12" fill="#6b7280">${{tickValue}}</text>
        `;
      }}).join("");

      const axisSvg = `
        <line x1="${{margin.left}}" y1="${{margin.top + chartHeight}}" x2="${{margin.left + chartWidth}}" y2="${{margin.top + chartHeight}}" stroke="#cfd8db" stroke-width="1" />
        <line x1="${{margin.left}}" y1="${{margin.top}}" x2="${{margin.left}}" y2="${{margin.top + chartHeight}}" stroke="#cfd8db" stroke-width="1" />
      `;

      const totalFeatureCount = toNumber(rows[0].total_feature_count);
      const totalY = yScale(totalFeatureCount);
      const totalRuleSvg = `
        <line x1="${{margin.left}}" y1="${{totalY}}" x2="${{margin.left + chartWidth}}" y2="${{totalY}}" stroke="#64748b" stroke-width="2" stroke-dasharray="6 4" />
        <text x="${{margin.left + 8}}" y="${{Math.max(totalY - 8, margin.top + 12)}}" font-size="12" font-weight="700" fill="#475569">总 API 数 ${{totalFeatureCount}}</text>
      `;

      const barsSvg = rows.map((item) => {{
        const versionIndex = versions.indexOf(String(item.version));
        const modeIndex = modes.indexOf(String(item.mode));
        const x = barX(versionIndex, modeIndex);
        const y = yScale(item.supported_count);
        const heightValue = Math.max(margin.top + chartHeight - y, 2);
        return `
          <rect x="${{x}}" y="${{y}}" width="${{barWidth}}" height="${{heightValue}}" rx="4" fill="${{modeColors[item.mode] || '#176b72'}}">
            <title>${{escapeHtml(item.mode)}} / ${{escapeHtml(item.version)}}: ${{item.supported_count}} (${{item.supported_percentage}}%)</title>
          </rect>
          <text x="${{x + barWidth / 2}}" y="${{y - 10}}" text-anchor="middle" font-size="11" font-weight="700" fill="${{modeColors[item.mode] || '#176b72'}}">${{escapeHtml(item.point_label || "")}}</text>
          ${{
            String(item.version) === "26ai" && String(item.delta_label || "")
              ? `<text x="${{x + barWidth / 2}}" y="${{y - 28}}" text-anchor="middle" font-size="11" font-weight="700" fill="${{modeColors[item.mode] || '#176b72'}}">${{escapeHtml(item.delta_label)}}</text>`
              : ""
          }}
        `;
      }}).join("");

      const labelsSvg = versions.map((version, index) => {{
        const x = groupCenter(index);
        return `<text x="${{x}}" y="${{margin.top + chartHeight + 28}}" text-anchor="middle" font-size="12" font-weight="700" fill="#475569">${{escapeHtml(version)}}</text>`;
      }}).join("");

      const legendHtml = modes.map((mode) => `
        <div class="chart-legend-item">
          <span class="chart-legend-swatch" style="background:${{modeColors[mode]}};"></span>
          <span>${{escapeHtml(mode)}}</span>
        </div>
      `).join("");

      target.innerHTML = `
        <h3>${{escapeHtml(title)}}</h3>
        <svg class="chart-svg" viewBox="0 0 ${{width}} ${{height}}" role="img" aria-label="${{escapeHtml(title)}}">
          ${{gridSvg}}
          ${{axisSvg}}
          ${{totalRuleSvg}}
          ${{barsSvg}}
          ${{labelsSvg}}
        </svg>
        <div class="chart-legend">${{legendHtml}}</div>
      `;
    }}

    function updateMultiSelectButton(container) {{
      const selected = selectedValues(container);
      const button = container.querySelector(".multi-select-button");
      const placeholder = container.dataset.placeholder || "请选择";
      if (!selected.length) {{
        button.textContent = placeholder;
        return;
      }}
      if (selected.length === 1) {{
        button.textContent = selected[0];
        return;
      }}
      button.textContent = `已选择 ${{selected.length}} 项`;
    }}

    function populateSelect(container, values, placeholder, selected = []) {{
      const menuContent = values.length
        ? values.map((value, index) => `
            <label class="multi-select-option" for="${{container.id}}-option-${{index}}">
              <input id="${{container.id}}-option-${{index}}" type="checkbox" value="${{escapeHtml(value)}}" />
              <span>${{escapeHtml(value)}}</span>
            </label>
          `).join("")
        : `<div class="multi-select-empty">暂无可选项</div>`;

      container.dataset.placeholder = placeholder;
      container.innerHTML = `
        <button type="button" class="multi-select-button">${{escapeHtml(placeholder)}}</button>
        <div class="multi-select-menu">${{menuContent}}</div>
      `;

      const button = container.querySelector(".multi-select-button");
      button.addEventListener("click", (event) => {{
        event.stopPropagation();
        document.querySelectorAll(".multi-select.open").forEach((element) => {{
          if (element !== container) {{
            element.classList.remove("open");
          }}
        }});
        container.classList.toggle("open");
      }});

      container.querySelectorAll("input[type='checkbox']").forEach((input) => {{
        input.checked = selected.includes(input.value);
        input.addEventListener("change", () => {{
          updateMultiSelectButton(container);
          applyFilters();
        }});
      }});

      updateMultiSelectButton(container);
    }}

    function refreshStatusFilterOptions() {{
      const selected = selectedValues(statusFilter);
      const availableStatuses = uniqueSorted(
        reportData.detailRows
          .map(withEffectiveStatus)
          .map((row) => String(row.__status || "").trim() || "Unknown")
      );
      populateSelect(statusFilter, availableStatuses, "选择支持判断", selected);
    }}

    function buildFilterText(row) {{
      const searchColumns = ["Command", "Operator", "Stage", "功能说明", "MongoDB 官方文档"];
      return searchColumns
        .map((column) => String(row[column] || ""))
        .join(" ")
        .toLowerCase();
    }}

    function applyFilters() {{
      const keyword = keywordFilter.value.trim().toLowerCase();
      const selectedSections = new Set(selectedValues(sectionFilter));
      const selectedStatuses = new Set(selectedValues(statusFilter));
      const onlyWithDescription = descOnlyFilter.checked;
      const effectiveRows = reportData.detailRows.map(withEffectiveStatus);

      const filteredRows = effectiveRows.filter((row) => {{
        if (selectedSections.size && !selectedSections.has(row.__section || "")) {{
          return false;
        }}
        if (selectedStatuses.size && !selectedStatuses.has(row.__status || "")) {{
          return false;
        }}
        if (onlyWithDescription && !String(row["功能说明"] || "").trim()) {{
          return false;
        }}
        if (keyword && !buildFilterText(row).includes(keyword)) {{
          return false;
        }}
        return true;
      }});

      renderSummaries(effectiveRows);
      renderDetails(filteredRows);
    }}

    function renderSummaries(rows) {{
      const statusRows = aggregateCounts(rows, "__status", "Other");
      document.getElementById("status-context-note").textContent =
        `当前支持判断：Oracle ${{currentOracleTargetVersion()}} + ${{currentOracleTargetMode()}}`;

      renderStatusChart(
        "status-chart",
        "支持情况分析",
        statusRows,
        (item) => statusColorMap[item.label] || statusColorMap.Other
      );
      const totalFeatureCount = rows.length;
      const modeVersionRows = ["op", "no-op"].flatMap((mode) =>
        ["19c", "26ai"].map((version) => {{
          const supportedCount = rows.filter((row) =>
            effectiveOracleSupportStatus(row.__support_since || "", version, mode) === "Supported"
          ).length;
          return {{
            mode,
            version,
            supported_count: supportedCount,
            supported_percentage: totalFeatureCount ? ((supportedCount / totalFeatureCount) * 100).toFixed(1) : "0.0",
            point_label: `${{supportedCount}} / ${{totalFeatureCount ? ((supportedCount / totalFeatureCount) * 100).toFixed(1) : "0.0"}}%`,
            total_feature_count: totalFeatureCount,
          }};
        }})
      );
      modeVersionRows.forEach((item) => {{
        const baseline = modeVersionRows.find((candidate) => candidate.mode === item.mode && candidate.version === "19c");
        item.delta_label = item.version === "26ai" ? `${{item.supported_count - (baseline ? baseline.supported_count : 0) >= 0 ? "+" : ""}}${{item.supported_count - (baseline ? baseline.supported_count : 0)}}` : "";
      }});
      renderModeVersionChart(
        "support-chart",
        "模式与版本演进",
        modeVersionRows
      );
    }}

    function renderDetails(rows) {{
      const target = document.getElementById("detail-sections");
      if (!rows.length) {{
        target.innerHTML = "<p class='empty-state'>当前筛选条件下没有命中记录。</p>";
        return;
      }}

      const grouped = new Map();
      rows.forEach((row) => {{
        const section = row.__section || "Unknown Section";
        if (!grouped.has(section)) {{
          grouped.set(section, []);
        }}
        grouped.get(section).push(row);
      }});

      const sectionsHtml = Array.from(grouped.entries()).map(([section, sectionRows]) => {{
        const visibleColumns = reportData.detailColumns.filter((column) => (
          sectionRows.some((row) => String(row[column] || "").trim() !== "")
        ));
        const orderedColumns = reorderDetailColumns(visibleColumns, section);
        const tableRows = sectionRows.map((row) => {{
          const record = {{}};
          orderedColumns.forEach((column) => {{
            record[column] = row[column] || "";
          }});
          return record;
        }});
        return `
          <details class="section-group">
            <summary>
              <span>${{escapeHtml(section)}}</span>
              ${{sectionStatusBar(sectionRows)}}
              <span class="section-count">${{sectionRows.length}} 条</span>
            </summary>
            <div class="section-body table-wrap">
              ${{createTable(orderedColumns, tableRows, ["MongoDB 官方文档"])}}
            </div>
          </details>
        `;
      }}).join("");

      target.innerHTML = sectionsHtml;
    }}

    function expandOrCollapseAll(open) {{
      document.querySelectorAll(".section-group").forEach((element) => {{
        element.open = open;
      }});
    }}

    function clearFilters() {{
      keywordFilter.value = "";
      sectionFilter.querySelectorAll("input[type='checkbox']").forEach((input) => {{
        input.checked = false;
      }});
      statusFilter.querySelectorAll("input[type='checkbox']").forEach((input) => {{
        input.checked = false;
      }});
      updateMultiSelectButton(sectionFilter);
      updateMultiSelectButton(statusFilter);
      descOnlyFilter.checked = false;
      applyFilters();
    }}

    function init() {{
      const oracleVersions = ["任意版本", ...availableOracleVersions()];
      oracleVersionFilter.innerHTML = oracleVersions
        .map((value) => `<option value="${{escapeHtml(value)}}">${{escapeHtml(value)}}</option>`)
        .join("");
      oracleVersionFilter.value = reportData.oracleTargetVersion || "任意版本";
      oracleModeFilter.value = reportData.oracleTargetMode || "任意部署方式";
      populateSelect(sectionFilter, uniqueSorted(reportData.detailRows.map((row) => row.__section || "Unknown Section")), "选择 section");
      refreshStatusFilterOptions();

      oracleVersionFilter.addEventListener("change", () => {{
        refreshStatusFilterOptions();
        applyFilters();
      }});
      oracleModeFilter.addEventListener("change", () => {{
        refreshStatusFilterOptions();
        applyFilters();
      }});
      keywordFilter.addEventListener("input", applyFilters);
      descOnlyFilter.addEventListener("change", applyFilters);
      clearFiltersButton.addEventListener("click", clearFilters);
      expandAllButton.addEventListener("click", () => expandOrCollapseAll(true));
      collapseAllButton.addEventListener("click", () => expandOrCollapseAll(false));
      document.addEventListener("click", (event) => {{
        if (!sectionFilter.contains(event.target)) {{
          sectionFilter.classList.remove("open");
        }}
        if (!statusFilter.contains(event.target)) {{
          statusFilter.classList.remove("open");
        }}
      }});

      applyFilters();
    }}

    init();
  </script>
</body>
</html>
"""


def _build_usage_offline_report_html(
    output_dir: str,
    metadata: dict[str, object],
    detail_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    source_detail_row_count: int,
    target_version: str,
    target_mode: str,
    selected_statuses: list[str],
    selected_op_types: list[str],
    selected_feature_types: list[str],
    selected_commands: list[str],
    selected_complexities: list[str],
    selected_scopes: list[str],
    keyword: str,
    show_baseline_advanced_columns: bool,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_mode_display = _oracle_target_mode_label(target_mode)
    safe_detail_df = detail_df.copy().fillna("")
    safe_baseline_df = baseline_df.copy().fillna("")
    payload = {
        "generatedAt": generated_at,
        "outputDir": output_dir,
        "metadata": {
            "database_name": str(metadata.get("database_name", "") or ""),
            "fetched_at": str(metadata.get("fetched_at", "") or ""),
            "start_time": str(metadata.get("start_time", "") or ""),
            "end_time": str(metadata.get("end_time", "") or ""),
            "profile_count": int(metadata.get("profile_count", 0) or 0),
            "truncated": bool(metadata.get("truncated", False)),
            "unknown_command_event_count": int(metadata.get("unknown_command_event_count", 0) or 0),
            "unmapped_feature_count": int(metadata.get("unmapped_feature_count", 0) or 0),
        },
        "oracleTargetVersion": str(target_version or "任意版本"),
        "oracleTargetMode": str(target_mode_display or "任意部署方式"),
        "sourceDetailRowCount": int(source_detail_row_count or 0),
        "initialFilters": {
            "statuses": [str(item) for item in selected_statuses],
            "opTypes": [str(item) for item in selected_op_types],
            "featureTypes": [str(item) for item in selected_feature_types],
            "commands": [str(item) for item in selected_commands],
            "complexities": [str(item) for item in selected_complexities],
            "scopes": [str(item) for item in selected_scopes],
            "keyword": str(keyword or ""),
        },
        "showBaselineAdvancedColumns": bool(show_baseline_advanced_columns),
        "detailRows": safe_detail_df.astype(object).to_dict(orient="records"),
        "baselineRows": safe_baseline_df.astype(object).to_dict(orient="records"),
    }

    return f"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MongoDB Usage 分析</title>
  <style>
    :root {{
      --bg: #f4f7f8;
      --panel: #ffffff;
      --panel-soft: #f8fbfb;
      --line: #dde5e7;
      --text: #15202b;
      --muted: #57727a;
      --brand-1: #0f3b45;
      --brand-2: #176b72;
      --brand-3: #2f8f83;
      --shadow: 0 10px 28px rgba(12, 52, 59, 0.06);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      background:
        radial-gradient(circle at top left, rgba(47, 143, 131, 0.08), transparent 28%),
        radial-gradient(circle at top right, rgba(15, 59, 69, 0.08), transparent 26%),
        var(--bg);
      color: var(--text);
    }}
    .page {{ max-width: min(96vw, 1600px); margin: 0 auto; padding: 24px; }}
    .hero {{
      background: linear-gradient(135deg, #0f3b45 0%, #176b72 56%, #2f8f83 100%);
      color: #fff;
      border-radius: 8px;
      padding: 24px;
      margin-bottom: 20px;
      box-shadow: var(--shadow);
    }}
    .hero h1 {{ margin: 0 0 8px 0; font-size: 28px; }}
    .meta-grid {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}
    .panel-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }}
    .meta-card, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      box-shadow: var(--shadow);
    }}
    .meta-card .label {{ color: var(--muted); font-size: 13px; margin-bottom: 6px; }}
    .meta-card .value {{ font-size: 22px; font-weight: 700; }}
    .panel {{ margin-bottom: 18px; }}
    .panel h2 {{ margin: 0 0 12px 0; font-size: 20px; }}
    .filter-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }}
    .filter-control label {{
      display: block;
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 6px;
      font-weight: 600;
    }}
    .text-input {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel-soft);
      color: var(--text);
      padding: 10px 12px;
      font-size: 14px;
    }}
    .multi-select {{ position: relative; }}
    .multi-select-button {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel-soft);
      color: var(--text);
      padding: 10px 12px;
      font-size: 14px;
      text-align: left;
      cursor: pointer;
    }}
    .multi-select-button::after {{ content: "▾"; float: right; color: var(--muted); }}
    .multi-select.open .multi-select-button::after {{ content: "▴"; }}
    .multi-select-menu {{
      display: none;
      position: absolute;
      left: 0;
      right: 0;
      top: calc(100% + 6px);
      z-index: 20;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      max-height: 260px;
      overflow: auto;
      padding: 8px;
    }}
    .multi-select.open .multi-select-menu {{ display: block; }}
    .multi-select-option {{
      display: flex;
      align-items: flex-start;
      gap: 8px;
      padding: 8px 6px;
      border-radius: 6px;
      font-size: 14px;
      cursor: pointer;
    }}
    .multi-select-option:hover {{ background: #eef5f5; }}
    .multi-select-option input {{ margin-top: 2px; }}
    .multi-select-empty {{ color: var(--muted); font-size: 13px; padding: 8px 6px; }}
    .report-table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    .report-table th, .report-table td {{
      border: 1px solid var(--line);
      padding: 8px 10px;
      vertical-align: top;
      text-align: left;
    }}
    .report-table th {{ background: #f1f5f6; position: sticky; top: 0; }}
    .table-wrap {{ overflow-x: auto; max-height: 520px; }}
    .note {{ color: var(--muted); font-size: 13px; line-height: 1.5; }}
    .empty-state {{ color: var(--muted); margin: 0; }}
    .code-block {{
      background: #0f1720;
      color: #d7e3ea;
      border-radius: 8px;
      padding: 12px;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 13px;
    }}
    @media (max-width: 980px) {{
      .meta-grid, .panel-grid, .filter-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="hero">
      <h1>MongoDB Usage 分析</h1>
      <div>离线报告</div>
      <div class="note">生成时间：{generated_at}</div>
    </div>

    <section class="panel">
      <h2>结果概览</h2>
      <div class="meta-grid">
        <div class="meta-card"><div class="label">全部唯一 API 数量</div><div class="value" id="metric-baseline-count">0</div></div>
        <div class="meta-card"><div class="label">实际使用 API 数量</div><div class="value" id="metric-usage-count">0</div></div>
        <div class="meta-card"><div class="label">高复杂度 API</div><div class="value" id="metric-high-complexity">0</div></div>
        <div class="meta-card"><div class="label">热点项数量</div><div class="value" id="metric-hotspot-count">0</div></div>
        <div class="meta-card"><div class="label">已观察到的基准 API</div><div class="value" id="metric-observed-baseline">0</div></div>
      </div>
      <div class="note">数据库：{html_lib.escape(str(metadata.get("database_name", "") or ""))} | 抓取时间：{html_lib.escape(str(metadata.get("fetched_at", "") or ""))} | 开始时间：{html_lib.escape(str(metadata.get("start_time", "") or "未设置"))} | 结束时间：{html_lib.escape(str(metadata.get("end_time", "") or "未设置"))}</div>
      <div class="note">当前支持判断：Oracle {html_lib.escape(str(target_version or "任意版本"))} + {html_lib.escape(target_mode_display)}</div>
      <div class="note">结果目录：{html_lib.escape(output_dir)}</div>
    </section>

    <section class="panel">
      <h2>分析筛选</h2>
      <div class="filter-grid">
        <div class="filter-control">
          <label for="oracle-version-filter">目标 Oracle 数据库版本</label>
          <select id="oracle-version-filter" class="text-input"></select>
        </div>
        <div class="filter-control">
          <label for="oracle-mode-filter">部署方式</label>
          <select id="oracle-mode-filter" class="text-input">
            <option value="任意部署方式">任意部署方式</option>
            <option value="op">op</option>
            <option value="no-op">no-op</option>
          </select>
        </div>
        <div class="filter-control">
          <label for="keyword-filter">关键字搜索</label>
          <input id="keyword-filter" class="text-input" type="text" placeholder="搜索 feature / collection / op / Oracle 分类 / 迁移动作" />
        </div>
        <div class="filter-control">
          <label>支持判断说明</label>
          <div class="note">离线报告固定为导出时页面所选的 Oracle 版本和部署方式，其余筛选与页面保持一致。</div>
        </div>
      </div>
      <div class="filter-grid">
        <div class="filter-control"><label>按当前组合支持判断筛选</label><div id="status-filter" class="multi-select"></div></div>
        <div class="filter-control"><label>按 Profile op 筛选</label><div id="op-filter" class="multi-select"></div></div>
        <div class="filter-control"><label>按功能类型筛选</label><div id="feature-type-filter" class="multi-select"></div></div>
        <div class="filter-control"><label>按命令筛选</label><div id="command-filter" class="multi-select"></div></div>
      </div>
      <div class="filter-grid">
        <div class="filter-control"><label>按迁移复杂度筛选</label><div id="complexity-filter" class="multi-select"></div></div>
        <div class="filter-control"><label>按迁移必要性筛选</label><div id="scope-filter" class="multi-select"></div></div>
        <div class="filter-control" style="grid-column: span 2;">
          <label>筛选说明</label>
          <div class="note" id="usage-context-note"></div>
        </div>
      </div>
    </section>

    <section class="panel">
      <h2>API 基准</h2>
      <div class="meta-grid">
        <div class="meta-card"><div class="label">文档同步明细记录数</div><div class="value" id="metric-source-detail-count">0</div></div>
        <div class="meta-card"><div class="label">全部唯一 API 数量</div><div class="value" id="metric-baseline-total">0</div></div>
        <div class="meta-card"><div class="label">实际使用到的 API 数量</div><div class="value" id="metric-baseline-observed">0</div></div>
      </div>
      <div class="note">展示文档同步得到的全部 MongoDB API 的默认迁移必要性和迁移复杂度评估。只要当前目标组合下 Oracle 标记为 Supported，默认复杂度降为 Low。</div>
      <label class="note" style="display:flex;align-items:center;gap:8px;margin:12px 0;">
        <input id="baseline-advanced-columns-toggle" type="checkbox" />
        显示高级列
      </label>
      <div id="baseline-table" class="table-wrap"></div>
    </section>

    <section class="panel">
      <h2>实际使用 API</h2>
      <div class="meta-grid">
        <div class="meta-card"><div class="label">实际使用 API 数量</div><div class="value" id="metric-workload-count">0</div></div>
        <div class="meta-card"><div class="label">高复杂度 API</div><div class="value" id="metric-workload-high">0</div></div>
        <div class="meta-card"><div class="label">热点项数量</div><div class="value" id="metric-workload-hotspots">0</div></div>
      </div>
      <div class="note">展示当前 system.profile 中实际观察到的 API，并同时给出迁移必要性、复杂度和建议动作。只要当前目标组合下 Oracle 标记为 Supported，默认复杂度降为 Low。</div>
      <div id="usage-detail-table" class="table-wrap"></div>
    </section>

    <section class="panel">
      <h2>证据样本</h2>
      <div class="filter-control" style="max-width:640px;">
        <label for="evidence-select">选择一项实际使用 API 查看证据</label>
        <select id="evidence-select" class="text-input"></select>
      </div>
      <div class="panel-grid" style="margin-top:16px;">
        <section class="panel" style="margin-bottom:0;">
          <h2 style="font-size:18px;">摘要</h2>
          <div id="evidence-summary" class="note">当前筛选条件下没有可展示的证据样本。</div>
        </section>
        <section class="panel" style="margin-bottom:0;">
          <h2 style="font-size:18px;">样本值</h2>
          <div id="evidence-sample-path" class="note"></div>
          <div id="evidence-sample-value" class="code-block">暂无样本。</div>
        </section>
      </div>
    </section>
  </div>
  <script>
    const reportData = {_json_for_html(payload)};
    const oracleVersionFilter = document.getElementById("oracle-version-filter");
    const oracleModeFilter = document.getElementById("oracle-mode-filter");
    const keywordFilter = document.getElementById("keyword-filter");
    const statusFilter = document.getElementById("status-filter");
    const opFilter = document.getElementById("op-filter");
    const featureTypeFilter = document.getElementById("feature-type-filter");
    const commandFilter = document.getElementById("command-filter");
    const complexityFilter = document.getElementById("complexity-filter");
    const scopeFilter = document.getElementById("scope-filter");
    const evidenceSelect = document.getElementById("evidence-select");
    const baselineAdvancedColumnsToggle = document.getElementById("baseline-advanced-columns-toggle");

    function escapeHtml(value) {{
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }}
    function selectedValues(container) {{
      return Array.from(container.querySelectorAll("input[type='checkbox']:checked")).map((input) => input.value);
    }}
    function uniqueSorted(values) {{
      return [...new Set(values.filter((value) => String(value || "").trim() !== ""))]
        .sort((left, right) => String(left).localeCompare(String(right), "zh-CN"));
    }}
    function withEffectiveStatus(row) {{
      return {{ ...row }};
    }}
    function currentOracleTargetVersion() {{
      return reportData.oracleTargetVersion || "任意版本";
    }}
    function currentOracleTargetMode() {{
      return reportData.oracleTargetMode || "任意部署方式";
    }}
    function createTable(columns, rows) {{
      if (!rows.length) return "<p class='empty-state'>暂无数据</p>";
      const thead = `<thead><tr>${{columns.map((column) => `<th>${{escapeHtml(column)}}</th>`).join("")}}</tr></thead>`;
      const tbody = rows.map((row) => `<tr>${{columns.map((column) => `<td>${{escapeHtml(row[column] ?? "")}}</td>`).join("")}}</tr>`).join("");
      return `<table class="report-table">${{thead}}<tbody>${{tbody}}</tbody></table>`;
    }}
    function populateSelect(container, values, placeholder, selected = []) {{
      const menuContent = values.length
        ? values.map((value, index) => `
            <label class="multi-select-option" for="${{container.id}}-option-${{index}}">
              <input id="${{container.id}}-option-${{index}}" type="checkbox" value="${{escapeHtml(value)}}" ${{selected.includes(value) ? "checked" : ""}} />
              <span>${{escapeHtml(value)}}</span>
            </label>`).join("")
        : `<div class="multi-select-empty">暂无可选项</div>`;
      container.dataset.placeholder = placeholder;
      container.innerHTML = `<button type="button" class="multi-select-button">${{escapeHtml(placeholder)}}</button><div class="multi-select-menu">${{menuContent}}</div>`;
      const button = container.querySelector(".multi-select-button");
      button.addEventListener("click", (event) => {{
        event.stopPropagation();
        document.querySelectorAll(".multi-select.open").forEach((element) => {{
          if (element !== container) element.classList.remove("open");
        }});
        container.classList.toggle("open");
      }});
      container.querySelectorAll("input[type='checkbox']").forEach((input) => {{
        input.addEventListener("change", () => {{
          updateMultiSelectButton(container);
          applyFilters();
        }});
      }});
      updateMultiSelectButton(container);
    }}
    function updateMultiSelectButton(container) {{
      const selected = selectedValues(container);
      const button = container.querySelector(".multi-select-button");
      const placeholder = container.dataset.placeholder || "请选择";
      if (!selected.length) button.textContent = placeholder;
      else if (selected.length === 1) button.textContent = selected[0];
      else button.textContent = `已选择 ${{selected.length}} 项`;
    }}
    function currentFilters() {{
      return {{
        statuses: new Set(selectedValues(statusFilter)),
        opTypes: new Set(selectedValues(opFilter)),
        featureTypes: new Set(selectedValues(featureTypeFilter)),
        commands: new Set(selectedValues(commandFilter)),
        complexities: new Set(selectedValues(complexityFilter)),
        scopes: new Set(selectedValues(scopeFilter)),
        keyword: keywordFilter.value.trim().toLowerCase(),
      }};
    }}
    function uniqueApiCount(rows) {{
      if (!rows.length) return 0;
      const keys = new Set();
      rows.forEach((row) => {{
        const scopeKey = String(row.oracle_section || row.oracle_category || "").trim();
        const supportSince = String(row.oracle_support_since || "").trim();
        const featureType = String(row.feature_type || "").trim();
        const featureName = String(row.feature_name || "").trim();
        keys.add([featureType, featureName, scopeKey, supportSince].join("||"));
      }});
      return keys.size;
    }}
    function buildUsageFilterText(row) {{
      return ["feature_name", "collection", "op_type", "command_name", "oracle_category", "oracle_support_since", "oracle_support_status", "effective_complexity", "effective_migration_necessity", "recommended_action"]
        .map((column) => String(row[column] || "")).join(" ").toLowerCase();
    }}
    function buildBaselineFilterText(row) {{
      return ["feature_name", "feature_type", "oracle_section", "oracle_support_since", "oracle_support_statuses", "mongo_short_description", "effective_complexity", "effective_migration_necessity", "complexity_explanation", "observed_command_contexts"]
        .map((column) => String(row[column] || "")).join(" ").toLowerCase();
    }}
    function refreshStatusFilterOptions() {{
      const selected = selectedValues(statusFilter);
      const baselineStatuses = reportData.baselineRows.flatMap((row) => String(row.oracle_support_statuses || "").split(",").map((item) => item.trim()).filter(Boolean));
      const usageStatuses = reportData.detailRows.map(withEffectiveStatus).map((row) => row.oracle_support_status || "Unknown");
      const availableStatuses = uniqueSorted([...baselineStatuses, ...usageStatuses]);
      populateSelect(statusFilter, availableStatuses, "选择支持判断", selected.filter((item) => availableStatuses.includes(item)));
    }}
    function effectiveUsageRows() {{
      return reportData.detailRows.map(withEffectiveStatus);
    }}
    function filteredUsageRows() {{
      const filters = currentFilters();
      return effectiveUsageRows().filter((row) => {{
        if (filters.statuses.size && !filters.statuses.has(row.oracle_support_status || "")) return false;
        if (filters.opTypes.size && !filters.opTypes.has(String(row.op_type || ""))) return false;
        if (filters.featureTypes.size && !filters.featureTypes.has(String(row.feature_type || ""))) return false;
        if (filters.commands.size && !filters.commands.has(String(row.command_name || ""))) return false;
        if (filters.complexities.size && !filters.complexities.has(String(row.effective_complexity || ""))) return false;
        if (filters.scopes.size && !filters.scopes.has(String(row.effective_migration_necessity || row.effective_scope || ""))) return false;
        if (filters.keyword && !buildUsageFilterText(row).includes(filters.keyword)) return false;
        return true;
      }});
    }}
    function filteredBaselineRows() {{
      const filters = currentFilters();
      return reportData.baselineRows.filter((row) => {{
        if (filters.featureTypes.size && !filters.featureTypes.has(String(row.feature_type || ""))) return false;
        if (filters.complexities.size && !filters.complexities.has(String(row.effective_complexity || ""))) return false;
        if (filters.scopes.size && !filters.scopes.has(String(row.effective_migration_necessity || row.effective_scope || ""))) return false;
        if (filters.statuses.size) {{
          const statuses = new Set(String(row.oracle_support_statuses || "").split(",").map((item) => item.trim()).filter(Boolean));
          if (![...filters.statuses].some((status) => statuses.has(status))) return false;
        }}
        if (filters.commands.size) {{
          const values = new Set(
            [String(row.feature_name || ""), String(row.observed_command_contexts || "")]
              .join(",")
              .split(",")
              .map((item) => item.trim())
              .filter(Boolean)
          );
          if (![...filters.commands].some((command) => values.has(command))) return false;
        }}
        if (filters.keyword && !buildBaselineFilterText(row).includes(filters.keyword)) return false;
        return true;
      }});
    }}
    function isObservedBaselineRow(row) {{
      return row.observed_in_profile === true || ["true", "1"].includes(String(row.observed_in_profile || "").toLowerCase());
    }}
    function isHotspotRow(row) {{
      const complexity = String(row.effective_complexity || "");
      const usageCount = Number(row.usage_count || 0);
      return complexity === "High" || complexity === "Blocker" || (complexity === "Medium" && usageCount >= 20);
    }}
    function renderOverview(usageRows, baselineRows) {{
      document.getElementById("metric-baseline-count").textContent = String(uniqueApiCount(baselineRows));
      document.getElementById("metric-usage-count").textContent = String(uniqueApiCount(usageRows));
      document.getElementById("metric-high-complexity").textContent = String(
        uniqueApiCount(usageRows.filter((row) => ["High", "Blocker"].includes(String(row.effective_complexity || ""))))
      );
      document.getElementById("metric-hotspot-count").textContent = String(uniqueApiCount(usageRows.filter(isHotspotRow)));
      document.getElementById("metric-observed-baseline").textContent = String(uniqueApiCount(baselineRows.filter(isObservedBaselineRow)));
      document.getElementById("usage-context-note").textContent = `当前支持判断基于所选组合: Oracle ${{currentOracleTargetVersion()}} + ${{currentOracleTargetMode()}}`;
    }}
    function renderBaseline(rows) {{
      document.getElementById("metric-source-detail-count").textContent = String(reportData.sourceDetailRowCount || 0);
      document.getElementById("metric-baseline-total").textContent = String(uniqueApiCount(rows));
      document.getElementById("metric-baseline-observed").textContent = String(uniqueApiCount(rows.filter(isObservedBaselineRow)));
      const coreColumns = [
        "feature_type",
        "feature_name",
        "effective_migration_necessity",
        "oracle_support_since",
        "effective_complexity",
        "complexity_explanation",
        "override_complexity",
        "override_reason",
      ];
      const advancedColumns = [
        "oracle_section",
        "oracle_support_statuses",
        "mongo_short_description",
        "observed_in_profile",
        "observed_usage_count",
        "observed_command_contexts",
        "override_enabled",
      ];
      const columns = baselineAdvancedColumnsToggle.checked
        ? [...coreColumns, ...advancedColumns]
        : coreColumns;
      document.getElementById("baseline-table").innerHTML = createTable(columns, rows);
    }}
    function renderUsage(rows) {{
      document.getElementById("metric-workload-count").textContent = String(uniqueApiCount(rows));
      document.getElementById("metric-workload-high").textContent = String(
        uniqueApiCount(rows.filter((row) => ["High", "Blocker"].includes(String(row.effective_complexity || ""))))
      );
      document.getElementById("metric-workload-hotspots").textContent = String(uniqueApiCount(rows.filter(isHotspotRow)));
      const columns = [
        "feature_type",
        "feature_name",
        "command_name",
        "database",
        "collection",
        "usage_count",
        "max_duration_ms",
        "oracle_support_status",
        "effective_migration_necessity",
        "effective_complexity",
        "migration_priority",
        "recommended_action",
        "complexity_reason",
      ];
      document.getElementById("usage-detail-table").innerHTML = createTable(columns, rows);
    }}
    function renderEvidence(rows) {{
      const evidenceRows = [...rows].sort((left, right) => Number(right.usage_count || 0) - Number(left.usage_count || 0));
      if (!evidenceRows.length) {{
        evidenceSelect.innerHTML = `<option>当前筛选条件下没有可展示的证据样本</option>`;
        document.getElementById("evidence-summary").textContent = "当前筛选条件下没有可展示的证据样本。";
        document.getElementById("evidence-sample-path").textContent = "";
        document.getElementById("evidence-sample-value").textContent = "暂无样本。";
        return;
      }}
      const previousValue = evidenceSelect.value;
      evidenceSelect.innerHTML = evidenceRows.map((row, index) => {{
        const label = `${{row.feature_type}} | ${{row.feature_name}} | ${{row.command_name || '-'}} | ${{row.collection || '-'}}`;
        return `<option value="${{index}}">${{escapeHtml(label)}}</option>`;
      }}).join("");
      if (previousValue && Number(previousValue) < evidenceRows.length) evidenceSelect.value = previousValue;
      const selectedRow = evidenceRows[Number(evidenceSelect.value || 0)] || evidenceRows[0];
      document.getElementById("evidence-summary").innerHTML = `
        <div>- feature_type: ${{escapeHtml(selectedRow.feature_type || "")}}</div>
        <div>- feature_name: ${{escapeHtml(selectedRow.feature_name || "")}}</div>
        <div>- command_name: ${{escapeHtml(selectedRow.command_name || "")}}</div>
        <div>- op_type: ${{escapeHtml(selectedRow.op_type || "-")}}</div>
        <div>- database.collection: ${{escapeHtml(`${{selectedRow.database || ""}}.${{selectedRow.collection || ""}}`)}}</div>
        <div>- usage_count: ${{escapeHtml(String(selectedRow.usage_count || ""))}}</div>
        <div>- oracle_support_status: ${{escapeHtml(selectedRow.oracle_support_status || "")}}</div>
        <div>- oracle_support_since: ${{escapeHtml(selectedRow.oracle_support_since || "-")}}</div>
        <div>- oracle_category: ${{escapeHtml(selectedRow.oracle_category || "-")}}</div>
        <div>- effective_complexity: ${{escapeHtml(selectedRow.effective_complexity || "-")}}</div>
        <div>- recommended_action: ${{escapeHtml(selectedRow.recommended_action || "-")}}</div>
      `;
      document.getElementById("evidence-sample-path").innerHTML = `<strong>命令中的位置</strong>: <code>${{escapeHtml(selectedRow.sample_path || "")}}</code>`;
      document.getElementById("evidence-sample-value").textContent = String(selectedRow.sample_value || "暂无样本。");
    }}
    function applyFilters() {{
      const usageRows = filteredUsageRows();
      const baselineRows = filteredBaselineRows();
      renderOverview(usageRows, baselineRows);
      renderBaseline(baselineRows);
      renderUsage(usageRows);
      renderEvidence(usageRows);
    }}
    function init() {{
      oracleVersionFilter.innerHTML = `<option value="${{escapeHtml(reportData.oracleTargetVersion || "任意版本")}}">${{escapeHtml(reportData.oracleTargetVersion || "任意版本")}}</option>`;
      oracleModeFilter.innerHTML = `<option value="${{escapeHtml(reportData.oracleTargetMode || "任意部署方式")}}">${{escapeHtml(reportData.oracleTargetMode || "任意部署方式")}}</option>`;
      populateSelect(statusFilter, [], "选择支持判断", reportData.initialFilters.statuses || []);
      populateSelect(opFilter, uniqueSorted(reportData.detailRows.map((row) => row.op_type || "")), "选择 Profile op", reportData.initialFilters.opTypes || []);
      populateSelect(featureTypeFilter, uniqueSorted(reportData.detailRows.map((row) => row.feature_type || "")), "选择功能类型", reportData.initialFilters.featureTypes || []);
      populateSelect(commandFilter, uniqueSorted(reportData.detailRows.map((row) => row.command_name || "")), "选择命令", reportData.initialFilters.commands || []);
      populateSelect(complexityFilter, uniqueSorted(reportData.detailRows.map((row) => row.effective_complexity || "")), "选择迁移复杂度", reportData.initialFilters.complexities || []);
      populateSelect(scopeFilter, uniqueSorted([
        ...reportData.detailRows.map((row) => row.effective_migration_necessity || row.effective_scope || ""),
        ...reportData.baselineRows.map((row) => row.effective_migration_necessity || row.effective_scope || ""),
      ]), "选择迁移必要性", reportData.initialFilters.scopes || []);
      keywordFilter.value = reportData.initialFilters.keyword || "";
      baselineAdvancedColumnsToggle.checked = !!reportData.showBaselineAdvancedColumns;
      refreshStatusFilterOptions();
      keywordFilter.addEventListener("input", applyFilters);
      baselineAdvancedColumnsToggle.addEventListener("change", applyFilters);
      evidenceSelect.addEventListener("change", () => renderEvidence(filteredUsageRows()));
      document.addEventListener("click", (event) => {{
        [statusFilter, opFilter, featureTypeFilter, commandFilter, complexityFilter, scopeFilter].forEach((container) => {{
          if (!container.contains(event.target)) container.classList.remove("open");
        }});
      }});
      applyFilters();
    }}
    init();
  </script>
</body>
</html>
"""


def _latest_output_dir(output_root: str = "outputs", pattern: str = "feature_support_*") -> Path | None:
    root = Path(output_root)
    if not root.exists():
        return None
    candidates = [p for p in root.glob(pattern) if p.is_dir()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _restore_result_from_dir(cache_dir: Path) -> bool:
    if cache_dir is None or not cache_dir.exists() or not cache_dir.is_dir():
        return False

    detail_path = cache_dir / "feature_support_detail.csv"
    summary_path = cache_dir / "feature_support_summary.csv"
    metadata_path = cache_dir / "document_metadata.json"
    if not detail_path.exists() or not summary_path.exists():
        return False

    try:
        detail_df = pd.read_csv(detail_path, encoding="utf-8-sig")
        summary_df = pd.read_csv(summary_path, encoding="utf-8-sig")
        doc_metadata = (
            json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata_path.exists()
            else {}
        )
    except Exception:  # noqa: BLE001
        return False

    st.session_state.result_detail_df = detail_df
    st.session_state.result_summary_df = summary_df
    st.session_state.result_output_dir = str(cache_dir)
    st.session_state.doc_metadata = doc_metadata
    return True


def _restore_last_result() -> bool:
    latest_dir = _latest_output_dir(pattern="feature_support_*")
    if latest_dir is None:
        return False
    return _restore_result_from_dir(latest_dir)


def _oracle_cache_label(cache_dir: Path, fallback_mongodb_manual_version: str = "") -> str:
    metadata_path = cache_dir / "document_metadata.json"
    mongodb_manual_version = ""
    doc_version = ""
    try:
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            mongodb_manual_version = str(metadata.get("mongodb_manual_version", "") or "")
            doc_version = str(metadata.get("doc_version_date", "") or "")
    except Exception:  # noqa: BLE001
        mongodb_manual_version = ""
        doc_version = ""
    mongodb_version_text = mongodb_manual_version or str(fallback_mongodb_manual_version or "").strip() or "未同步"
    oracle_doc_text = doc_version or "未解析到"
    return f"{cache_dir.name} | MongoDB {mongodb_version_text} | Oracle {oracle_doc_text}"


def _restore_usage_result_from_dir(cache_dir: Path) -> bool:
    if cache_dir is None or not cache_dir.exists() or not cache_dir.is_dir():
        return False

    detail_path = cache_dir / "mongodb_usage_feature_detail.csv"
    summary_path = cache_dir / "mongodb_usage_feature_summary.csv"
    metadata_path = cache_dir / "mongodb_usage_metadata.json"
    migration_summary_path = cache_dir / "mongodb_migration_summary.csv"
    hotspots_path = cache_dir / "mongodb_migration_hotspots.csv"
    excluded_path = cache_dir / "mongodb_migration_excluded_commands.csv"
    if not detail_path.exists() or not summary_path.exists():
        return False

    try:
        detail_df = pd.read_csv(detail_path, encoding="utf-8-sig")
        summary_df = pd.read_csv(summary_path, encoding="utf-8-sig")
        migration_summary_df = (
            pd.read_csv(migration_summary_path, encoding="utf-8-sig")
            if migration_summary_path.exists()
            else None
        )
        hotspots_df = (
            pd.read_csv(hotspots_path, encoding="utf-8-sig")
            if hotspots_path.exists()
            else None
        )
        excluded_df = (
            pd.read_csv(excluded_path, encoding="utf-8-sig")
            if excluded_path.exists()
            else None
        )
        usage_metadata = (
            json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata_path.exists()
            else {}
        )
    except Exception:  # noqa: BLE001
        return False

    st.session_state.mongo_usage_detail_df = detail_df
    st.session_state.mongo_usage_summary_df = summary_df
    st.session_state.mongo_usage_output_dir = str(cache_dir)
    st.session_state.mongo_usage_metadata = usage_metadata
    st.session_state.mongo_usage_migration_summary_df = migration_summary_df
    st.session_state.mongo_usage_baseline_df = None
    st.session_state.mongo_usage_hotspots_df = hotspots_df
    st.session_state.mongo_usage_excluded_df = excluded_df
    st.session_state.mongo_usage_events_df = None
    return True


def _restore_last_usage_result() -> bool:
    latest_dir = _latest_output_dir(pattern="mongodb_usage_*")
    if latest_dir is None:
        return False
    return _restore_usage_result_from_dir(latest_dir)


def _usage_cache_label(cache_dir: Path) -> str:
    metadata_path = cache_dir / "mongodb_usage_metadata.json"
    database_name = ""
    start_time = ""
    end_time = ""
    try:
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            database_name = str(metadata.get("database_name", "") or "")
            start_time = str(metadata.get("start_time", "") or "")
            end_time = str(metadata.get("end_time", "") or "")
    except Exception:  # noqa: BLE001
        database_name = ""
        start_time = ""
        end_time = ""

    timestamp = datetime.fromtimestamp(cache_dir.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    window_text = ""
    if start_time or end_time:
        window_text = f" | {start_time or '未设置'} ~ {end_time or '未设置'}"
    db_text = f" | {database_name}" if database_name else ""
    return f"{cache_dir.name} | {timestamp}{db_text}{window_text}"


if (
    not st.session_state.restore_attempted
    and st.session_state.result_detail_df is None
    and st.session_state.result_summary_df is None
):
    st.session_state.restore_attempted = True
    st.session_state.restored_from_disk = _restore_last_result()

if (
    not st.session_state.mongo_usage_restore_attempted
    and st.session_state.mongo_usage_detail_df is None
    and st.session_state.mongo_usage_summary_df is None
):
    st.session_state.mongo_usage_restore_attempted = True
    st.session_state.mongo_usage_restored_from_disk = _restore_last_usage_result()


def emit_log(message: str) -> None:
    st.session_state.debug_logs.append(message)
    if st.session_state.get("show_debug_log_runtime", True):
        log_container.empty()
        with log_container.container():
            with st.expander("▸ 执行日志", expanded=True):
                st.code("\n".join(st.session_state.debug_logs[-200:]), language="text")

if sync_reference:
    st.session_state.debug_logs = []
    st.session_state.show_debug_log_runtime = show_debug_log
    _save_ui_settings()
    with st.spinner("正在同步 MongoDB 官方说明，请稍候..."):
        try:
            sync_result = sync_mongodb_reference_catalog(
                timeout=int(timeout),
                max_retries=int(max_retries),
                about_url=st.session_state.ui_saved_mongodb_url,
                source_overrides={
                    definition["id"]: st.session_state.ui_doc_link_urls.get(definition["id"], definition["default_url"])
                    for definition in _document_link_definitions()
                    if definition["id"] not in {"oracle_feature_support", "manual_version"}
                },
                progress_callback=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            st.error(f"MongoDB 官方说明同步失败: {exc}")
        else:
            st.session_state.reference_df = sync_result.reference_df
            st.session_state.reference_metadata = sync_result.metadata
            st.success(f"MongoDB 官方说明已同步，共 {sync_result.metadata.get('entry_count', 0)} 条。")

if submitted:
    _save_ui_settings()
    st.session_state.debug_logs = []
    st.session_state.show_debug_log_runtime = show_debug_log
    st.session_state.restored_from_disk = False
    st.session_state.oracle_cache_save_notice = ""
    st.session_state.result_detail_df = None
    st.session_state.result_summary_df = None
    st.session_state.result_output_dir = None
    st.session_state.doc_metadata = {}

    with st.spinner("正在同步 Oracle 官方文档，请稍候..."):
        try:
            result = analyze_feature_support(
                url=st.session_state.ui_saved_url,
                index_url=st.session_state.ui_doc_link_urls.get("oracle_version_source", INDEX_URL),
                timeout=int(timeout),
                max_retries=int(max_retries),
                progress_callback=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            st.error(f"执行失败: {exc}")
            st.info("建议先把“请求超时（秒）”调到 90 至 120，再重试。")
        else:
            st.session_state.result_detail_df = result.detail_df
            st.session_state.result_summary_df = result.summary_df
            st.session_state.result_output_dir = None
            st.session_state.doc_metadata = result.doc_metadata
            st.success("Oracle 官方文档已同步。")

with doc_left_panel:
    oracle_cache_dirs = _list_output_dirs("feature_support_*")
    oracle_cache_options = {
        str(path): _oracle_cache_label(
            path,
            fallback_mongodb_manual_version=str(
                (st.session_state.reference_metadata or {}).get("mongodb_manual_version", "") or ""
            ),
        )
        for path in oracle_cache_dirs
    }
    with st.container(border=True):
        header_cols = st.columns([1.0, 0.28], gap="small")
        with header_cols[0]:
            st.markdown('<div class="icon-toolbar-title" style="padding-top:0.35rem;">历史缓存</div>', unsafe_allow_html=True)
        with header_cols[1]:
            toolbar_cols = st.columns(2, gap="small")
            with toolbar_cols[0]:
                load_oracle_cache = st.button(
                    "",
                    key="load_oracle_cache",
                    help="加载所选缓存",
                    icon=":material/folder_open:",
                    disabled=not oracle_cache_options,
                    width="stretch",
                )
            with toolbar_cols[1]:
                clear_oracle_caches = st.button(
                    "",
                    key="clear_oracle_caches",
                    help="清理全部缓存",
                    icon=":material/delete:",
                    disabled=not oracle_cache_options,
                    width="stretch",
                )
        if oracle_cache_options:
            if st.session_state.oracle_selected_cache_dir not in oracle_cache_options:
                st.session_state.oracle_selected_cache_dir = next(iter(oracle_cache_options))
            selected_oracle_cache_dir = st.selectbox(
                "选择缓存版本",
                options=list(oracle_cache_options.keys()),
                format_func=lambda value: oracle_cache_options.get(value, value),
                key="oracle_selected_cache_dir",
            )
            if load_oracle_cache:
                target_dir = Path(selected_oracle_cache_dir)
                if _restore_result_from_dir(target_dir):
                    st.session_state.restored_from_disk = True
                    st.session_state.oracle_cache_save_notice = f"已加载本地缓存：{target_dir}"
                    st.rerun()
                st.error("加载所选缓存失败。")
            if clear_oracle_caches and not st.session_state.confirm_clear_oracle_caches:
                st.session_state.confirm_clear_oracle_caches = True
                st.rerun()
            if st.session_state.confirm_clear_oracle_caches:
                st.warning("确认清理全部文档同步缓存？此操作会删除本地保存的所有 feature_support_* 目录。")
                oracle_confirm_cols = st.columns(2, gap="medium")
                with oracle_confirm_cols[0]:
                    if st.button("确认清理", key="confirm_clear_oracle_caches_btn", width="stretch"):
                        removed_count = _clear_output_dirs("feature_support_*")
                        st.session_state.pop("oracle_selected_cache_dir", None)
                        st.session_state.confirm_clear_oracle_caches = False
                        st.session_state.oracle_cache_save_notice = f"已清理文档同步缓存，共删除 {removed_count} 个目录。"
                        if st.session_state.result_output_dir and not Path(str(st.session_state.result_output_dir)).exists():
                            st.session_state.result_output_dir = None
                            st.session_state.restored_from_disk = False
                        st.rerun()
                with oracle_confirm_cols[1]:
                    if st.button("取消", key="cancel_clear_oracle_caches_btn", width="stretch"):
                        st.session_state.confirm_clear_oracle_caches = False
                        st.rerun()
        else:
            st.caption("暂无可用缓存。")

with usage_tab:
    usage_left_col, usage_right_col = st.columns([1.0, 2.15], gap="large")
    with usage_left_col:
        usage_left_panel = st.container(height=WORKSPACE_PANEL_HEIGHT, border=False)
    with usage_right_col:
        usage_right_panel = st.container(height=WORKSPACE_PANEL_HEIGHT, border=False)

with usage_left_panel:
    with st.container(border=True):
        st.markdown("#### MongoDB Usage 分析")
        if st.session_state.result_detail_df is None:
            st.info("请先同步 Oracle 官方文档，再进行 Usage 分析。")

        def emit_mongo_trace(message: str) -> None:
            st.session_state.mongo_usage_trace_logs.append(message)

        with st.form("mongo_usage_analysis_form", clear_on_submit=False):
            mongodb_uri = st.text_input(
                "MongoDB URI",
                key="mongo_usage_uri",
                type="password",
                placeholder="mongodb://user:password@host:27017/oracle_mongo_api_test?authSource=admin",
            )
            mongo_sample_limit = st.number_input(
                "最大采样条数",
                min_value=100,
                max_value=200000,
                step=100,
                value=20000,
                key="mongo_usage_sample_limit",
            )

            time_cols = st.columns(2, gap="medium")
            with time_cols[0]:
                mongo_start_time = st.text_input(
                    "开始时间（可选）",
                    key="mongo_usage_start_time",
                    placeholder="2026-04-10T00:00:00",
                )
            with time_cols[1]:
                mongo_end_time = st.text_input(
                    "结束时间（可选）",
                    key="mongo_usage_end_time",
                    placeholder="2026-04-10T23:59:59",
                )

            mongo_usage_trace_enabled = st.checkbox(
                "显示日志",
                key="mongo_usage_trace_enabled",
            )
            action_cols = st.columns(2, gap="medium")
            with action_cols[0]:
                test_usage_connection = st.form_submit_button(
                    "测试连接",
                    width="stretch",
                )
            with action_cols[1]:
                analyze_usage = st.form_submit_button(
                    "分析 system.profile",
                    type="primary",
                    width="stretch",
                )

        if test_usage_connection or analyze_usage:
            st.session_state.mongo_usage_trace_logs = []
            _save_ui_settings()
            if not str(mongodb_uri).strip():
                st.error("请填写 MongoDB URI。")
            else:
                try:
                    effective_database_name = _resolve_mongodb_database_name(
                        str(mongodb_uri).strip(),
                        "",
                    )
                    if not effective_database_name:
                        raise ValueError("MongoDB URI 中必须包含默认数据库，例如 /oracle_mongo_api_test。")
                    start_time = None
                    end_time = None
                    if test_usage_connection or analyze_usage:
                        start_time = _parse_optional_datetime(mongo_start_time)
                        end_time = _parse_optional_datetime(mongo_end_time)
                        if start_time and end_time and start_time > end_time:
                            raise ValueError("开始时间不能晚于结束时间。")
                except ValueError as exc:
                    st.error(str(exc))
                else:
                    if test_usage_connection:
                        with st.spinner("正在测试 MongoDB 连接，请稍候..."):
                            try:
                                test_result = test_mongodb_connection(
                                    mongodb_uri=str(mongodb_uri).strip(),
                                    database_name=effective_database_name,
                                    progress_callback=emit_mongo_trace if mongo_usage_trace_enabled else None,
                                )
                            except Exception as exc:  # noqa: BLE001
                                st.session_state.mongo_usage_connection_test = {}
                                st.error(f"连接测试失败：{exc}")
                            else:
                                if test_result.has_system_profile:
                                    st.success(
                                        f"连接成功，当前数据库为 `{test_result.database_name}`，已检测到 system.profile。"
                                    )
                                else:
                                    st.warning(
                                        f"连接成功，当前数据库为 `{test_result.database_name}`，但未检测到 system.profile。请确认目标库已开启 profiler。"
                                    )
                    elif analyze_usage:
                        if st.session_state.result_detail_df is None:
                            st.error("当前没有 Oracle Feature Support 明细，请先同步 Oracle 官方文档。")
                        else:
                            with st.spinner("正在读取 system.profile 并生成使用报表，请稍候..."):
                                try:
                                    profile_result = read_system_profile(
                                        mongodb_uri=str(mongodb_uri).strip(),
                                        database_name=effective_database_name,
                                        start_time=start_time,
                                        end_time=end_time,
                                        limit=int(mongo_sample_limit),
                                        progress_callback=emit_mongo_trace if mongo_usage_trace_enabled else None,
                                    )
                                    if mongo_usage_trace_enabled:
                                        emit_mongo_trace("[USAGE] Normalize profile records")
                                    events = normalize_profile_records(profile_result.records)
                                    if mongo_usage_trace_enabled:
                                        emit_mongo_trace(f"[USAGE] Normalized {len(events)} event(s)")
                                        emit_mongo_trace("[USAGE] Convert normalized events to DataFrame")
                                    events_df = events_to_dataframe(events)
                                    if mongo_usage_trace_enabled:
                                        emit_mongo_trace("[USAGE] Extract MongoDB feature usage")
                                    usage_df = extract_feature_usages(events)
                                    if mongo_usage_trace_enabled:
                                        emit_mongo_trace(f"[USAGE] Extracted {len(usage_df)} feature row(s)")
                                        emit_mongo_trace("[USAGE] Map features to Oracle support detail")
                                    mapped_df = map_features_to_oracle_support(
                                        usage_df,
                                        st.session_state.result_detail_df,
                                    )
                                    if mongo_usage_trace_enabled:
                                        emit_mongo_trace("[USAGE] Load migration complexity rules")
                                    ruleset = load_migration_rules()
                                    if mongo_usage_trace_enabled:
                                        emit_mongo_trace(
                                            f"[USAGE] Apply migration assessment rules version={ruleset.rules_version or 'unknown'}"
                                        )
                                    migration_result = assess_migration_complexity(
                                        mapped_df,
                                        ruleset,
                                    )
                                    assessed_df = migration_result.detail_df
                                    if mongo_usage_trace_enabled:
                                        emit_mongo_trace("[USAGE] Build usage summary")
                                    summary_df = build_usage_summary(assessed_df)
                                    unknown_command_event_count = (
                                        int(events_df["command_name"].fillna("").astype(str).eq("unknown").sum())
                                        if not events_df.empty and "command_name" in events_df.columns
                                        else 0
                                    )
                                    unmapped_feature_count = (
                                        int(mapped_df["oracle_support_status"].fillna("").astype(str).eq("Unknown").sum())
                                        if not mapped_df.empty and "oracle_support_status" in mapped_df.columns
                                        else 0
                                    )
                                    metadata = {
                                        "database_name": effective_database_name,
                                        "fetched_at": profile_result.fetched_at,
                                        "start_time": start_time.isoformat() if start_time else "",
                                        "end_time": end_time.isoformat() if end_time else "",
                                        "profile_count": len(profile_result.records),
                                        "truncated": profile_result.truncated,
                                        "unknown_command_event_count": unknown_command_event_count,
                                        "unmapped_feature_count": unmapped_feature_count,
                                        "rules_version": migration_result.rules_version,
                                        "override_count": migration_result.override_count,
                                        "rules_coverage_rate": migration_result.rules_coverage_rate,
                                        "unclassified_feature_count": migration_result.unclassified_feature_count,
                                    }
                                except Exception as exc:  # noqa: BLE001
                                    st.error(f"Usage 分析失败：{exc}")
                                else:
                                    st.session_state.mongo_usage_cache_save_notice = ""
                                    st.session_state.mongo_usage_restored_from_disk = False
                                    st.session_state.mongo_usage_detail_df = assessed_df
                                    st.session_state.mongo_usage_summary_df = summary_df
                                    st.session_state.mongo_usage_migration_summary_df = migration_result.summary_df
                                    st.session_state.mongo_usage_baseline_df = migration_result.baseline_df
                                    st.session_state.mongo_usage_hotspots_df = migration_result.hotspots_df
                                    st.session_state.mongo_usage_excluded_df = migration_result.excluded_df
                                    st.session_state.mongo_usage_events_df = events_df
                                    st.session_state.mongo_usage_output_dir = None
                                    st.session_state.mongo_usage_metadata = metadata
                                    st.session_state.mongo_usage_override_save_notice = ""
                                    st.success("Usage 分析完成。")

        usage_cache_dirs = _list_output_dirs("mongodb_usage_*")
        cache_options = {str(path): _usage_cache_label(path) for path in usage_cache_dirs}
        with st.container(border=True):
            header_cols = st.columns([1.0, 0.28], gap="small")
            with header_cols[0]:
                st.markdown('<div class="icon-toolbar-title" style="padding-top:0.35rem;">历史缓存</div>', unsafe_allow_html=True)
            with header_cols[1]:
                toolbar_cols = st.columns(2, gap="small")
                with toolbar_cols[0]:
                    load_usage_cache = st.button(
                        "",
                        key="load_usage_cache",
                        help="加载所选缓存",
                        icon=":material/folder_open:",
                        disabled=not cache_options,
                        width="stretch",
                    )
                with toolbar_cols[1]:
                    clear_usage_caches = st.button(
                        "",
                        key="clear_usage_caches",
                        help="清理全部缓存",
                        icon=":material/delete:",
                        disabled=not cache_options,
                        width="stretch",
                    )
            if cache_options:
                if st.session_state.mongo_usage_selected_cache_dir not in cache_options:
                    st.session_state.mongo_usage_selected_cache_dir = next(iter(cache_options))
                selected_cache_dir = st.selectbox(
                    "选择缓存版本",
                    options=list(cache_options.keys()),
                    format_func=lambda value: cache_options.get(value, value),
                    key="mongo_usage_selected_cache_dir",
                )
                if load_usage_cache:
                    target_dir = Path(selected_cache_dir)
                    if _restore_usage_result_from_dir(target_dir):
                        st.session_state.mongo_usage_restored_from_disk = True
                        st.session_state.mongo_usage_cache_save_notice = f"已加载本地缓存：{target_dir}"
                        st.session_state.mongo_usage_connection_test = {}
                        st.rerun()
                    st.error("加载所选缓存失败。")
                if clear_usage_caches and not st.session_state.confirm_clear_usage_caches:
                    st.session_state.confirm_clear_usage_caches = True
                    st.rerun()
                if st.session_state.confirm_clear_usage_caches:
                    st.warning("确认清理全部 MongoDB Usage 缓存？此操作会删除本地保存的所有 mongodb_usage_* 目录。")
                    usage_confirm_cols = st.columns(2, gap="medium")
                    with usage_confirm_cols[0]:
                        if st.button("确认清理", key="confirm_clear_usage_caches_btn", width="stretch"):
                            removed_count = _clear_output_dirs("mongodb_usage_*")
                            st.session_state.pop("mongo_usage_selected_cache_dir", None)
                            st.session_state.confirm_clear_usage_caches = False
                            st.session_state.mongo_usage_cache_save_notice = f"已清理 MongoDB Usage 缓存，共删除 {removed_count} 个目录。"
                            if st.session_state.mongo_usage_output_dir and not Path(str(st.session_state.mongo_usage_output_dir)).exists():
                                st.session_state.mongo_usage_output_dir = None
                                st.session_state.mongo_usage_restored_from_disk = False
                            st.rerun()
                    with usage_confirm_cols[1]:
                        if st.button("取消", key="cancel_clear_usage_caches_btn", width="stretch"):
                            st.session_state.confirm_clear_usage_caches = False
                            st.rerun()
            else:
                st.caption("暂无可用缓存。")

        if st.session_state.mongo_usage_trace_logs and st.session_state.get("mongo_usage_trace_enabled", False):
            with st.expander("▸ Usage 日志", expanded=True):
                st.code("\n".join(st.session_state.mongo_usage_trace_logs[-300:]), language="text")

with testkit_tab:
    testkit_left_col, testkit_right_col = st.columns([1.0, 2.15], gap="large")
    with testkit_left_col:
        testkit_left_panel = st.container(height=WORKSPACE_PANEL_HEIGHT, border=False)
    with testkit_right_col:
        testkit_right_panel = st.container(height=WORKSPACE_PANEL_HEIGHT, border=False)

with testkit_left_panel:
    with st.container(border=True):
        st.markdown("#### MongoDB 测试工具")
        st.warning("该区域会写入、覆盖集合，并临时调整 profiler。请仅连接测试库。")

        def emit_testkit_trace(message: str) -> None:
            st.session_state.mongo_testkit_trace_logs.append(message)

        with st.form("mongo_testkit_form", clear_on_submit=False):
            testkit_uri = st.text_input(
                "MongoDB URI",
                key="mongo_testkit_uri",
                type="password",
                placeholder="mongodb://user:password@host:27017/oracle_mongo_api_test?authSource=admin",
            )
            mongo_testkit_trace_enabled = st.checkbox(
                "显示日志",
                key="mongo_testkit_trace_enabled",
            )
            mongo_testkit_confirm = st.checkbox(
                "我确认当前连接的是测试库，允许应用写入和覆盖测试数据",
                key="mongo_testkit_confirm",
                help="初始化测试数据会重建 customers/orders/inventory/order_metrics/order_archive；运行测试查询会写入测试数据并临时调整 profiler。",
            )
            testkit_action_cols = st.columns(3, gap="medium")
            with testkit_action_cols[0]:
                test_testkit_connection = st.form_submit_button("测试连接", width="stretch")
            with testkit_action_cols[1]:
                init_testkit_data = st.form_submit_button("初始化测试数据", width="stretch")
            with testkit_action_cols[2]:
                run_testkit_queries = st.form_submit_button("运行测试查询", width="stretch")

        if test_testkit_connection or init_testkit_data or run_testkit_queries:
            st.session_state.mongo_testkit_trace_logs = []
            _save_ui_settings()
            if not str(testkit_uri).strip():
                st.error("请填写 MongoDB URI。")
            else:
                try:
                    effective_testkit_database = _resolve_mongodb_database_name(
                        str(testkit_uri).strip(),
                        "",
                    )
                    if not effective_testkit_database:
                        raise ValueError("MongoDB URI 中必须包含默认数据库，例如 /oracle_mongo_api_test。")
                except ValueError as exc:
                    st.error(str(exc))
                else:
                    if test_testkit_connection:
                        with st.spinner("正在测试 MongoDB 连接，请稍候..."):
                            try:
                                test_result = test_mongodb_connection(
                                    mongodb_uri=str(testkit_uri).strip(),
                                    database_name=effective_testkit_database,
                                    progress_callback=emit_testkit_trace if mongo_testkit_trace_enabled else None,
                                )
                            except Exception as exc:  # noqa: BLE001
                                st.session_state.mongo_testkit_connection_test = {}
                                st.error(f"连接测试失败：{exc}")
                            else:
                                if test_result.has_system_profile:
                                    st.success(
                                        f"连接成功，当前数据库为 `{test_result.database_name}`，已检测到 system.profile。"
                                    )
                                else:
                                    st.warning(
                                        f"连接成功，当前数据库为 `{test_result.database_name}`，但未检测到 system.profile。"
                                    )
                    elif init_testkit_data:
                        if not mongo_testkit_confirm:
                            st.error("请先勾选测试库确认项，再执行初始化测试数据。")
                        else:
                            with st.spinner("正在初始化测试数据，请稍候..."):
                                try:
                                    seed_result = seed_test_data(
                                        mongodb_uri=str(testkit_uri).strip(),
                                        database_name=effective_testkit_database,
                                        progress_callback=emit_testkit_trace if mongo_testkit_trace_enabled else None,
                                    )
                                except Exception as exc:  # noqa: BLE001
                                    st.error(f"初始化测试数据失败：{exc}")
                                else:
                                    st.session_state.mongo_testkit_seed_result = {
                                        "database_name": seed_result.database_name,
                                        "finished_at": seed_result.finished_at,
                                        "dropped_collections": seed_result.dropped_collections,
                                        "inserted_counts": seed_result.inserted_counts,
                                        "created_indexes": seed_result.created_indexes,
                                    }
                                    st.success("测试数据初始化完成。")
                    elif run_testkit_queries:
                        if not mongo_testkit_confirm:
                            st.error("请先勾选测试库确认项，再执行测试查询。")
                        else:
                            with st.spinner("正在执行测试查询，请稍候..."):
                                try:
                                    exercise_result = run_profile_exercises(
                                        mongodb_uri=str(testkit_uri).strip(),
                                        database_name=effective_testkit_database,
                                        progress_callback=emit_testkit_trace if mongo_testkit_trace_enabled else None,
                                    )
                                except Exception as exc:  # noqa: BLE001
                                    st.error(f"执行测试查询失败：{exc}")
                                else:
                                    st.session_state.mongo_testkit_exercise_result = {
                                        "database_name": exercise_result.database_name,
                                        "finished_at": exercise_result.finished_at,
                                        "command_count": exercise_result.command_count,
                                        "original_profile_level": exercise_result.original_profile_level,
                                        "restored_profile_level": exercise_result.restored_profile_level,
                                        "profile_count_before": exercise_result.profile_count_before,
                                        "profile_count_after": exercise_result.profile_count_after,
                                        "unsupported_feature_names": exercise_result.unsupported_feature_names,
                                    }
                                    st.success("测试查询执行完成。")

        if st.session_state.mongo_testkit_seed_result:
            seed_result = st.session_state.mongo_testkit_seed_result
            with st.container(border=True):
                st.markdown('<div class="panel-subsection-title">测试数据初始化结果</div>', unsafe_allow_html=True)
                seed_cols = st.columns(3, gap="medium")
                with seed_cols[0]:
                    st.metric("数据库", str(seed_result.get("database_name", "")))
                with seed_cols[1]:
                    inserted_total = sum(int(value or 0) for value in dict(seed_result.get("inserted_counts", {})).values())
                    st.metric("写入文档数", inserted_total)
                with seed_cols[2]:
                    st.metric("完成时间", str(seed_result.get("finished_at", "")))
                dropped_collections = seed_result.get("dropped_collections", [])
                if dropped_collections:
                    st.caption("重建集合: " + ", ".join(str(item) for item in dropped_collections))
                created_indexes = seed_result.get("created_indexes", {})
                if created_indexes:
                    index_parts: list[str] = []
                    for collection_name, index_names in dict(created_indexes).items():
                        if index_names:
                            index_parts.append(f"{collection_name}: {', '.join(str(name) for name in index_names)}")
                    if index_parts:
                        st.caption("索引: " + " | ".join(index_parts))

        if st.session_state.mongo_testkit_exercise_result:
            exercise_result = st.session_state.mongo_testkit_exercise_result
            with st.container(border=True):
                st.markdown('<div class="panel-subsection-title">测试查询执行结果</div>', unsafe_allow_html=True)
                exercise_cols = st.columns(4, gap="medium")
                with exercise_cols[0]:
                    st.metric("数据库", str(exercise_result.get("database_name", "")))
                with exercise_cols[1]:
                    st.metric("执行命令数", int(exercise_result.get("command_count", 0)))
                with exercise_cols[2]:
                    profile_delta = int(exercise_result.get("profile_count_after", 0)) - int(exercise_result.get("profile_count_before", 0))
                    st.metric("新增 profile 条数", profile_delta)
                with exercise_cols[3]:
                    st.metric("完成时间", str(exercise_result.get("finished_at", "")))
                st.caption(
                    "Profiler 恢复: "
                    + f"{exercise_result.get('original_profile_level', 0)} -> "
                    + f"{exercise_result.get('restored_profile_level', 0)}"
                )
                unsupported_feature_names = exercise_result.get("unsupported_feature_names", [])
                if unsupported_feature_names:
                    st.caption("预置的 Oracle 明确不支持样本: " + ", ".join(str(item) for item in unsupported_feature_names))

        if st.session_state.mongo_testkit_trace_logs and st.session_state.get("mongo_testkit_trace_enabled", False):
            with st.expander("▸ 测试工具日志", expanded=True):
                st.code("\n".join(st.session_state.mongo_testkit_trace_logs[-300:]), language="text")

with testkit_right_panel:
    with st.container(border=True):
        st.markdown("#### 使用说明")
        st.markdown(
            "\n".join(
                [
                    "- 该页只用于测试库准备，不用于迁移评估。",
                    "- 初始化集合：`customers`、`orders`、`inventory`、`order_metrics`、`order_archive`。",
                    "- 查询覆盖：`find`、`aggregate`、`update`、`delete`、`findAndModify`、`distinct`、`count`、`listIndexes`、`createIndexes`、`dropIndexes`。",
                    "- 不支持样本：`$expr`、`$bucketAuto`、`$graphLookup`、`$regexMatch`、`$setField`。",
                    "- `运行测试查询` 会临时把 profiler 调整为 level 2，执行完成后恢复。",
                    "- 建议先初始化测试数据，再运行测试查询，最后回到 `MongoDB Usage 分析` 页执行迁移评估。",
                ]
            )
        )

with doc_right_panel:
    if st.session_state.get("show_debug_log_runtime", True) and st.session_state.debug_logs:
        log_container.empty()
        with log_container.container():
            with st.expander("▸ 执行日志", expanded=True):
                st.code("\n".join(st.session_state.debug_logs[-200:]), language="text")

with doc_right_panel:
    if st.session_state.result_detail_df is not None and st.session_state.result_summary_df is not None:
        detail_df = st.session_state.result_detail_df
        output_dir = st.session_state.result_output_dir or ""
        doc_metadata = st.session_state.doc_metadata or {}
        reference_df = st.session_state.reference_df
        reference_metadata = st.session_state.reference_metadata or {}
        enriched_detail_df = (
            enrich_feature_support_detail(detail_df, reference_df)
            if not reference_df.empty
            else detail_df.copy()
        )
        oracle_version_options = _available_oracle_versions(
            enriched_detail_df["Support (Since)"].fillna("").astype(str).tolist()
            if "Support (Since)" in enriched_detail_df.columns
            else []
        )
        if st.session_state.feature_detail_target_version not in ["任意版本", *oracle_version_options]:
            st.session_state.feature_detail_target_version = "任意版本"
        selected_oracle_target_version = st.session_state.feature_detail_target_version
        selected_oracle_target_mode = _normalize_support_mode(
            st.session_state.feature_detail_target_mode
        )
        effective_detail_df = enriched_detail_df.copy()
        if "Support (Since)" in effective_detail_df.columns:
            effective_detail_df["normalized_status"] = effective_detail_df["Support (Since)"].map(
                lambda value: _effective_oracle_support_status(
                    value,
                    selected_oracle_target_version,
                    selected_oracle_target_mode,
                )
            )
        description_match_count = (
            effective_detail_df["mongo_short_description"].fillna("").astype(str).str.strip().ne("").sum()
            if "mongo_short_description" in effective_detail_df.columns
            else 0
        )
        status_summary_df = (
            effective_detail_df["normalized_status"]
            .fillna("Unknown")
            .astype(str)
            .value_counts(dropna=False)
            .rename_axis("normalized_status")
            .reset_index(name="count")
        )
        if not status_summary_df.empty:
            status_summary_df["percentage"] = (
                status_summary_df["count"] / status_summary_df["count"].sum() * 100
            ).round(2)
        support_top_display_df = pd.DataFrame()
        if "Support (Since)" in effective_detail_df.columns:
            support_top_display_df = (
                effective_detail_df["Support (Since)"]
                .fillna("Unknown")
                .astype(str)
                .value_counts(dropna=False)
                .head(20)
                .rename_axis("support_since")
                .reset_index(name="count")
            )
            if not support_top_display_df.empty:
                support_top_display_df["percentage"] = (
                    support_top_display_df["count"] / support_top_display_df["count"].sum() * 100
                ).round(2)
        support_mode_version_df = pd.DataFrame()
        if "Support (Since)" in effective_detail_df.columns and not effective_detail_df.empty:
            support_since_series = effective_detail_df["Support (Since)"].fillna("").astype(str)
            total_feature_count = len(support_since_series)
            scenario_rows: list[dict[str, object]] = []
            for mode_value, mode_label in [("op", "op"), ("no-op", "no-op")]:
                for version_value in ["19c", "26ai"]:
                    scenario_statuses = support_since_series.map(
                        lambda value: _effective_oracle_support_status(
                            value,
                            version_value,
                            mode_value,
                        )
                    )
                    supported_count = int(scenario_statuses.eq("Supported").sum())
                    partial_count = int(scenario_statuses.eq("Partially Supported").sum())
                    not_supported_count = int(scenario_statuses.eq("Not Supported").sum())
                    unknown_count = int(scenario_statuses.eq("Unknown").sum())
                    supported_percentage = (
                        round(supported_count / total_feature_count * 100, 2)
                        if total_feature_count
                        else 0.0
                    )
                    scenario_rows.append(
                        {
                            "mode": mode_label,
                            "version": version_value,
                            "supported_count": supported_count,
                            "supported_percentage": supported_percentage,
                            "partially_supported_count": partial_count,
                            "not_supported_count": not_supported_count,
                            "unknown_count": unknown_count,
                            "point_label": f"{supported_count} / {supported_percentage:.1f}%",
                            "version_sort": _oracle_version_rank(version_value) or (9999, 9999),
                        }
                    )
            if scenario_rows:
                support_mode_version_df = pd.DataFrame(scenario_rows).sort_values(
                    by=["mode", "version_sort"],
                    ascending=[True, True],
                ).reset_index(drop=True)
                baseline_counts = (
                    support_mode_version_df[support_mode_version_df["version"] == "19c"][
                        ["mode", "supported_count"]
                    ]
                    .rename(columns={"supported_count": "baseline_supported_count"})
                )
                support_mode_version_df = support_mode_version_df.merge(
                    baseline_counts,
                    on="mode",
                    how="left",
                )
                support_mode_version_df["delta_count"] = (
                    support_mode_version_df["supported_count"]
                    - support_mode_version_df["baseline_supported_count"].fillna(0)
                ).astype(int)
                support_mode_version_df["delta_label"] = support_mode_version_df.apply(
                    lambda row: (
                        f"{row['delta_count']:+d}"
                        if str(row["version"]) == "26ai"
                        else ""
                    ),
                    axis=1,
                )
                support_mode_version_df = support_mode_version_df.drop(columns=["version_sort"])
                support_mode_version_df["total_feature_count"] = total_feature_count
                support_mode_version_df["total_label"] = f"总 API 数 {total_feature_count}"

        section_display_df = pd.DataFrame()
        if "section" in effective_detail_df.columns:
            section_display_df = (
                effective_detail_df["section"]
                .fillna("Unknown Section")
                .astype(str)
                .value_counts(dropna=False)
                .rename_axis("section")
                .reset_index(name="count")
            )
            if not section_display_df.empty:
                section_display_df["percentage"] = (
                    section_display_df["count"] / section_display_df["count"].sum() * 100
                ).round(2)

        (
            current_detail_keyword,
            current_selected_sections,
            current_selected_statuses,
        ) = _current_feature_detail_filters(effective_detail_df)
        current_filtered_detail_df = _filter_detail_df(
            effective_detail_df,
            keyword=current_detail_keyword,
            selected_sections=current_selected_sections,
            selected_statuses=current_selected_statuses,
        )
        current_status_summary_df = (
            current_filtered_detail_df["normalized_status"]
            .fillna("Unknown")
            .astype(str)
            .value_counts(dropna=False)
            .rename_axis("normalized_status")
            .reset_index(name="count")
        )
        if not current_status_summary_df.empty:
            current_status_summary_df["percentage"] = (
                current_status_summary_df["count"] / current_status_summary_df["count"].sum() * 100
            ).round(2)
        status_summary_display_df = _prepare_status_summary_display_df(status_summary_df)
        current_status_summary_display_df = _prepare_status_summary_display_df(current_status_summary_df)

        current_support_top_display_df = pd.DataFrame()
        if "Support (Since)" in current_filtered_detail_df.columns:
            current_support_top_display_df = (
                current_filtered_detail_df["Support (Since)"]
                .fillna("Unknown")
                .astype(str)
                .value_counts(dropna=False)
                .head(20)
                .rename_axis("support_since")
                .reset_index(name="count")
            )
            if not current_support_top_display_df.empty:
                current_support_top_display_df["percentage"] = (
                    current_support_top_display_df["count"]
                    / current_support_top_display_df["count"].sum()
                    * 100
                ).round(2)

        current_section_display_df = pd.DataFrame()
        if "section" in current_filtered_detail_df.columns:
            current_section_display_df = (
                current_filtered_detail_df["section"]
                .fillna("Unknown Section")
                .astype(str)
                .value_counts(dropna=False)
                .rename_axis("section")
                .reset_index(name="count")
            )
            if not current_section_display_df.empty:
                current_section_display_df["percentage"] = (
                    current_section_display_df["count"]
                    / current_section_display_df["count"].sum()
                    * 100
                ).round(2)

        offline_report_html = _build_offline_report_html(
            output_dir=output_dir,
            doc_metadata=doc_metadata,
            reference_metadata=reference_metadata,
            status_summary_df=current_status_summary_df,
            support_top_display_df=current_support_top_display_df,
            section_display_df=current_section_display_df,
            filtered_detail_df=current_filtered_detail_df,
            oracle_target_version=selected_oracle_target_version,
            oracle_target_mode=selected_oracle_target_mode,
        )
        oracle_workbook_bytes = _build_excel_workbook_bytes(
            [
                ("detail", current_filtered_detail_df),
                ("summary", current_status_summary_display_df),
            ]
        )

        overview_cols = st.columns(4, gap="medium")
        with overview_cols[0]:
            st.metric("明细记录数", len(effective_detail_df))
        with overview_cols[1]:
            st.metric("Oracle 文档版本时间", doc_metadata.get("doc_version_date", "") or "未解析到")
        with overview_cols[2]:
            st.metric("Oracle 文档编号", doc_metadata.get("doc_id", "") or "未解析到")
        with overview_cols[3]:
            st.metric(
                "MongoDB 手册版本",
                str(reference_metadata.get("mongodb_manual_version", "")) or "未同步",
            )

        if st.session_state.restored_from_disk and output_dir:
            last_refresh_time = datetime.fromtimestamp(Path(output_dir).stat().st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            st.caption(f"上次刷新：{last_refresh_time}")
        elif st.session_state.oracle_cache_save_notice:
            st.caption(st.session_state.oracle_cache_save_notice)
        else:
            st.caption("当前结果尚未写入本地缓存。")

        download_cols = st.columns([0.65, 0.65, 0.65, 3.05], gap="medium")
        with download_cols[0]:
            if st.button(
                "",
                key="save_oracle_cache",
                help="保存当前结果到本地缓存",
                icon=":material/save:",
                width="stretch",
            ):
                saved_dir = _persist_oracle_cache(
                    detail_df=st.session_state.result_detail_df,
                    summary_df=st.session_state.result_summary_df,
                    doc_metadata={
                        **doc_metadata,
                        "mongodb_manual_version": str(
                            reference_metadata.get("mongodb_manual_version", "") or ""
                        ),
                    },
                    report_html=offline_report_html,
                    workbook_bytes=oracle_workbook_bytes,
                )
                st.session_state.result_output_dir = str(saved_dir)
                st.session_state.restored_from_disk = False
                st.session_state.oracle_cache_save_notice = f"已保存本地缓存：{saved_dir}"
                st.rerun()
        with download_cols[1]:
            st.download_button(
                "",
                data=oracle_workbook_bytes,
                file_name="feature_support_analysis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="下载 Excel（detail + summary）",
                icon=":material/grid_on:",
                width="stretch",
            )
        with download_cols[2]:
            st.download_button(
                "",
                data=offline_report_html.encode("utf-8"),
                file_name="feature_support_report.html",
                mime="text/html",
                help="下载 HTML 报告",
                icon=":material/language:",
                width="stretch",
            )
        with download_cols[3]:
            st.markdown("")

        selected_oracle_target_version, selected_oracle_target_mode = _render_oracle_target_controls(
            "feature_detail_target_version",
            "feature_detail_target_mode",
            oracle_version_options,
            "按客户计划部署的 Oracle 版本和部署方式，评估 Oracle Database API for MongoDB 在该场景下的兼容程度。",
        )

        with st.container(border=True):
            support_header_cols = st.columns([9.2, 0.8], gap="small", vertical_alignment="center")
            with support_header_cols[0]:
                st.markdown('<div class="panel-section-title">支持情况分析</div>', unsafe_allow_html=True)
            with support_header_cols[1]:
                if st.button(
                    "",
                    key="toggle_support_analysis",
                    help="展开或收起支持情况分析",
                    icon=":material/expand_more:" if st.session_state.support_analysis_expanded else ":material/chevron_right:",
                    width="stretch",
                ):
                    st.session_state.support_analysis_expanded = not st.session_state.support_analysis_expanded
                    st.rerun()

            if st.session_state.support_analysis_expanded:
                support_mode_label = _oracle_target_mode_label(selected_oracle_target_mode)
                support_combo_label = (
                    f"Oracle {selected_oracle_target_version} + {support_mode_label}"
                )
                st.caption(f"当前支持判断：{support_combo_label}")
                support_status_tab, support_evolution_tab = st.tabs(
                    ["状态汇总", "模式与版本演进"]
                )
                with support_status_tab:
                    status_color_domain = ["Supported", "Not Supported", "Partially Supported", "Other"]
                    status_color_range = ["#2E8B57", "#D9534F", "#F0AD4E", "#7A7A7A"]
                    status_label_df = status_summary_display_df.copy()
                    status_label_df["标签"] = status_label_df.apply(
                        lambda row: f"{int(row['数量'])} | {float(row['占比(%)']):.1f}%",
                        axis=1,
                    )
                    status_chart = (
                        alt.Chart(status_summary_display_df)
                        .mark_bar(cornerRadiusEnd=4, size=26)
                        .encode(
                            y=alt.Y(
                                "是否支持:N",
                                sort="-x",
                                title=None,
                                axis=alt.Axis(labelLimit=220),
                            ),
                            x=alt.X("数量:Q", title="数量", axis=alt.Axis(grid=True, tickMinStep=1)),
                            color=alt.Color(
                                "是否支持:N",
                                scale=alt.Scale(domain=status_color_domain, range=status_color_range),
                                legend=None,
                            ),
                            tooltip=["是否支持", "数量", "占比(%)"],
                        )
                    )
                    status_labels = (
                        alt.Chart(status_label_df)
                        .mark_text(
                            align="left",
                            baseline="middle",
                            dx=8,
                            fontSize=12,
                            fontWeight=700,
                            color="#31434a",
                        )
                        .encode(
                            y=alt.Y("是否支持:N", sort="-x"),
                            x=alt.X("数量:Q"),
                            text="标签:N",
                        )
                    )
                    st.altair_chart((status_chart + status_labels).properties(height=220), width="stretch")

                with support_evolution_tab:
                    if support_mode_version_df.empty:
                        st.info("没有可展示的版本演进统计。")
                    else:
                        evolution_color_domain = ["op", "no-op"]
                        evolution_color_range = ["#176b72", "#d97706"]
                        evolution_bars = (
                            alt.Chart(support_mode_version_df)
                            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, size=44)
                            .encode(
                                x=alt.X(
                                    "version:N",
                                    sort=["19c", "26ai"],
                                    title="Oracle 版本",
                                ),
                                y=alt.Y("supported_count:Q", title="Supported API 数"),
                                color=alt.Color(
                                    "mode:N",
                                    title="部署方式",
                                    scale=alt.Scale(
                                        domain=evolution_color_domain,
                                        range=evolution_color_range,
                                    ),
                                ),
                                xOffset=alt.XOffset("mode:N"),
                                tooltip=[
                                    alt.Tooltip("mode:N", title="部署方式"),
                                    alt.Tooltip("version:N", title="Oracle 版本"),
                                    alt.Tooltip("supported_count:Q", title="Supported API 数"),
                                    alt.Tooltip("supported_percentage:Q", title="Supported 占比(%)"),
                                    alt.Tooltip(
                                        "partially_supported_count:Q",
                                        title="Partially Supported 数",
                                    ),
                                    alt.Tooltip(
                                        "not_supported_count:Q",
                                        title="Not Supported 数",
                                    ),
                                    alt.Tooltip("unknown_count:Q", title="Unknown 数"),
                                ],
                            )
                        )
                        total_reference_line = (
                            alt.Chart(support_mode_version_df.iloc[:1])
                            .mark_rule(strokeDash=[6, 4], color="#64748b", strokeWidth=2)
                            .encode(y=alt.Y("total_feature_count:Q"))
                        )
                        total_reference_label = (
                            alt.Chart(support_mode_version_df.iloc[:1])
                            .mark_text(
                                align="left",
                                baseline="bottom",
                                dx=10,
                                dy=-6,
                                fontSize=11,
                                fontWeight=700,
                                color="#475569",
                            )
                            .encode(
                                x=alt.value(0),
                                y=alt.Y("total_feature_count:Q"),
                                text="total_label:N",
                            )
                        )
                        evolution_op_labels = (
                            alt.Chart(support_mode_version_df[support_mode_version_df["mode"] == "op"])
                            .mark_text(
                                dy=-14,
                                fontSize=11,
                                fontWeight=700,
                                color="#176b72",
                            )
                            .encode(
                                x=alt.X("version:N", sort=["19c", "26ai"]),
                                xOffset=alt.XOffset("mode:N", sort=evolution_color_domain),
                                y=alt.Y("supported_count:Q"),
                                text="point_label:N",
                            )
                        )
                        evolution_noop_labels = (
                            alt.Chart(support_mode_version_df[support_mode_version_df["mode"] == "no-op"])
                            .mark_text(
                                dy=-14,
                                fontSize=11,
                                fontWeight=700,
                                color="#d97706",
                            )
                            .encode(
                                x=alt.X("version:N", sort=["19c", "26ai"]),
                                xOffset=alt.XOffset("mode:N", sort=evolution_color_domain),
                                y=alt.Y("supported_count:Q"),
                                text="point_label:N",
                            )
                        )
                        evolution_op_delta_labels = (
                            alt.Chart(
                                support_mode_version_df[
                                    (support_mode_version_df["mode"] == "op")
                                    & (support_mode_version_df["version"] == "26ai")
                                ]
                            )
                            .mark_text(
                                dx=12,
                                dy=-32,
                                fontSize=11,
                                fontWeight=700,
                                color="#176b72",
                            )
                            .encode(
                                x=alt.X("version:N", sort=["19c", "26ai"]),
                                xOffset=alt.XOffset("mode:N", sort=evolution_color_domain),
                                y=alt.Y("supported_count:Q"),
                                text="delta_label:N",
                            )
                        )
                        evolution_noop_delta_labels = (
                            alt.Chart(
                                support_mode_version_df[
                                    (support_mode_version_df["mode"] == "no-op")
                                    & (support_mode_version_df["version"] == "26ai")
                                ]
                            )
                            .mark_text(
                                dx=12,
                                dy=-32,
                                fontSize=11,
                                fontWeight=700,
                                color="#d97706",
                            )
                            .encode(
                                x=alt.X("version:N", sort=["19c", "26ai"]),
                                xOffset=alt.XOffset("mode:N", sort=evolution_color_domain),
                                y=alt.Y("supported_count:Q"),
                                text="delta_label:N",
                            )
                        )
                        evolution_chart = alt.layer(
                            total_reference_line,
                            total_reference_label,
                            evolution_bars,
                            evolution_op_labels,
                            evolution_noop_labels,
                            evolution_op_delta_labels,
                            evolution_noop_delta_labels,
                        )
                        st.altair_chart(evolution_chart.properties(height=320), width="stretch")

        with st.container(border=True):
            st.markdown('<div id="feature-support-detail" class="panel-anchor"></div>', unsafe_allow_html=True)
            detail_header_cols = st.columns([9.2, 0.8], gap="small", vertical_alignment="center")
            with detail_header_cols[0]:
                st.markdown('<div class="panel-section-title">Feature Support 明细</div>', unsafe_allow_html=True)
            with detail_header_cols[1]:
                if st.button(
                    "",
                    key="toggle_feature_detail",
                    help="展开或收起 Feature Support 明细",
                    icon=":material/expand_more:" if st.session_state.feature_detail_expanded else ":material/chevron_right:",
                    width="stretch",
                ):
                    st.session_state.feature_detail_expanded = not st.session_state.feature_detail_expanded
                    st.rerun()

            if st.session_state.feature_detail_expanded:
                feature_detail_mode_label = _oracle_target_mode_label(selected_oracle_target_mode)
                st.caption(
                    f"当前支持判断：Oracle {selected_oracle_target_version} + {feature_detail_mode_label}"
                )
                detail_filter_row = st.columns([1.15, 1.0, 0.9], gap="medium")
                with detail_filter_row[0]:
                    detail_keyword = st.text_input(
                        "关键字搜索",
                        placeholder="搜索 Command / Operator / Stage / 功能说明",
                        key="detail_keyword",
                    )
                section_options = (
                    sorted(effective_detail_df["section"].dropna().astype(str).unique().tolist())
                    if "section" in effective_detail_df.columns
                    else []
                )
                with detail_filter_row[1]:
                    selected_sections = st.multiselect(
                        "按 section 筛选",
                        options=section_options,
                        key="detail_sections",
                        placeholder="全部 section",
                    )
                detail_status_options = (
                    sorted(effective_detail_df["normalized_status"].dropna().astype(str).unique().tolist())
                    if "normalized_status" in effective_detail_df.columns
                    else []
                )
                with detail_filter_row[2]:
                    selected_detail_statuses = st.multiselect(
                        "按当前组合支持判断筛选",
                        options=[option for option in detail_status_options if option],
                        key="detail_statuses",
                        placeholder="全部支持状态",
                    )

                filtered_detail_df = _filter_detail_df(
                    effective_detail_df,
                    keyword=detail_keyword,
                    selected_sections=selected_sections,
                    selected_statuses=selected_detail_statuses,
                )
                detail_section_count = (
                    int(filtered_detail_df["section"].fillna("").astype(str).nunique())
                    if "section" in filtered_detail_df.columns else 0
                )
                st.caption(f"筛选结果: {len(filtered_detail_df)} 条记录 | {detail_section_count} 个 section")

                detail_section_tab, detail_table_tab = st.tabs(["按 section 浏览", "全部明细"])
                with detail_section_tab:
                    if "section" in filtered_detail_df.columns:
                        grouped = list(filtered_detail_df.groupby("section", dropna=False, sort=False))
                        for section_name, sec_df in grouped:
                            section_text = str(section_name) if str(section_name).strip() else "Unknown Section"
                            expanded = bool(st.session_state.detail_expanded_sections.get(section_text, False))
                            with st.container(border=True):
                                section_header_row = st.columns([3.8, 0.9, 4.5, 0.8], gap="small", vertical_alignment="center")
                                with section_header_row[0]:
                                    st.markdown(f"**{section_text}**")
                                with section_header_row[1]:
                                    st.caption(f"{len(sec_df)} 条")
                                with section_header_row[2]:
                                    st.markdown(
                                        _section_status_bar_html(sec_df),
                                        unsafe_allow_html=True,
                                    )
                                with section_header_row[3]:
                                    if st.button(
                                        "",
                                        key=f"toggle_section_{section_text}",
                                        help=f"展开或收起 {section_text}",
                                        icon=":material/expand_more:" if expanded else ":material/chevron_right:",
                                        width="stretch",
                                    ):
                                        st.session_state.detail_expanded_sections[section_text] = not expanded
                                        st.rerun()
                                if not expanded:
                                    continue
                                sec_display_df = _prepare_detail_display_df(
                                    sec_df,
                                    include_section=False,
                                    section_name=section_text,
                                )
                                st.dataframe(
                                    sec_display_df,
                                    width="stretch",
                                    height=_dataframe_height(len(sec_display_df), max_height=360),
                                    hide_index=True,
                                )
                    else:
                        st.info("当前明细中没有可按 section 浏览的字段。")

                with detail_table_tab:
                    detail_display_df = _prepare_detail_display_df(
                        filtered_detail_df,
                        include_section=False,
                    )
                    st.dataframe(
                        detail_display_df,
                        width="stretch",
                        height=_dataframe_height(len(detail_display_df), max_height=440),
                        hide_index=True,
                    )

with usage_right_panel:
    if st.session_state.mongo_usage_detail_df is not None:
        usage_detail_df = st.session_state.mongo_usage_detail_df.copy()
        migration_summary_df = (
            st.session_state.mongo_usage_migration_summary_df.copy()
            if st.session_state.mongo_usage_migration_summary_df is not None
            else pd.DataFrame()
        )
        baseline_df = (
            st.session_state.mongo_usage_baseline_df.copy()
            if st.session_state.mongo_usage_baseline_df is not None
            else pd.DataFrame()
        )
        hotspots_df = (
            st.session_state.mongo_usage_hotspots_df.copy()
            if st.session_state.mongo_usage_hotspots_df is not None
            else pd.DataFrame()
        )
        excluded_df = (
            st.session_state.mongo_usage_excluded_df.copy()
            if st.session_state.mongo_usage_excluded_df is not None
            else pd.DataFrame()
        )
        usage_oracle_version_options = _available_oracle_versions(
            usage_detail_df["oracle_support_since"].fillna("").astype(str).tolist()
            if "oracle_support_since" in usage_detail_df.columns
            else []
        )
        if st.session_state.usage_analysis_target_version not in ["任意版本", *usage_oracle_version_options]:
            st.session_state.usage_analysis_target_version = "任意版本"
        usage_target_version = st.session_state.usage_analysis_target_version
        usage_target_mode = _normalize_support_mode(st.session_state.usage_analysis_target_mode)
        if "oracle_support_since" in usage_detail_df.columns:
            usage_detail_df["oracle_support_status"] = usage_detail_df["oracle_support_since"].map(
                lambda value: _effective_oracle_support_status(
                    value,
                    usage_target_version,
                    usage_target_mode,
                )
            )
        current_ruleset = load_migration_rules()
        reassessed_result = assess_migration_complexity(
            usage_detail_df,
            current_ruleset,
        )
        usage_detail_df = reassessed_result.detail_df
        migration_summary_df = reassessed_result.summary_df
        hotspots_df = reassessed_result.hotspots_df
        excluded_df = reassessed_result.excluded_df
        usage_summary_df = build_usage_summary(usage_detail_df)
        oracle_catalog_detail_df = (
            st.session_state.result_detail_df.copy()
            if st.session_state.result_detail_df is not None
            else pd.DataFrame()
        )
        if not oracle_catalog_detail_df.empty and not st.session_state.reference_df.empty:
            oracle_catalog_detail_df = enrich_feature_support_detail(
                oracle_catalog_detail_df,
                st.session_state.reference_df,
            )
        if not oracle_catalog_detail_df.empty and "Support (Since)" in oracle_catalog_detail_df.columns:
            oracle_catalog_detail_df = oracle_catalog_detail_df.copy()
            oracle_catalog_detail_df["normalized_status"] = oracle_catalog_detail_df["Support (Since)"].map(
                lambda value: _effective_oracle_support_status(
                    value,
                    usage_target_version,
                    usage_target_mode,
                )
            )
        catalog_usage_df = _build_catalog_usage_df(oracle_catalog_detail_df)
        catalog_baseline_result = assess_migration_complexity(
            catalog_usage_df,
            current_ruleset,
        )
        baseline_df = catalog_baseline_result.baseline_df
        if not baseline_df.empty:
            description_lookup = (
                oracle_catalog_detail_df.assign(
                    baseline_feature_type=lambda df: df.apply(
                        lambda item: "command"
                        if _safe_feature_text(item.get("Command", ""))
                        else (
                            "stage"
                            if _safe_feature_text(item.get("Stage", ""))
                            else (
                                "operator"
                                if _safe_feature_text(item.get("Operator", ""))
                                else ""
                            )
                        ),
                        axis=1,
                    ),
                    baseline_feature_name=lambda df: df.apply(
                        lambda item: _safe_feature_text(item.get("Command", ""))
                        or _safe_feature_text(item.get("Stage", ""))
                        or _safe_feature_text(item.get("Operator", "")),
                        axis=1,
                    ),
                    baseline_feature_section=lambda df: df["section"].fillna("").astype(str),
                    baseline_feature_support_since=lambda df: df["Support (Since)"].fillna("").astype(str),
                )
            )
            description_lookup = description_lookup[
                description_lookup["baseline_feature_type"].astype(str).str.strip().ne("")
                & description_lookup["baseline_feature_name"].astype(str).str.strip().ne("")
            ][
                [
                    "baseline_feature_type",
                    "baseline_feature_name",
                    "baseline_feature_section",
                    "baseline_feature_support_since",
                    "mongo_short_description",
                    "section",
                    "Support (Since)",
                ]
            ].drop_duplicates(
                subset=[
                    "baseline_feature_type",
                    "baseline_feature_name",
                    "baseline_feature_section",
                    "baseline_feature_support_since",
                ],
                keep="first",
            )
            baseline_df = baseline_df.merge(
                description_lookup.rename(
                    columns={
                        "baseline_feature_type": "feature_type",
                        "baseline_feature_name": "feature_name",
                        "baseline_feature_section": "oracle_section",
                        "baseline_feature_support_since": "oracle_support_since",
                        "mongo_short_description": "mongo_short_description",
                    }
                ),
                on=["feature_type", "feature_name", "oracle_section", "oracle_support_since"],
                how="left",
            )
            observed_lookup = (
                usage_detail_df.groupby(["feature_type", "feature_name"], dropna=False, as_index=False)
                .agg(
                    observed_usage_count=("usage_count", "sum"),
                    observed_command_contexts=(
                        "command_name",
                        lambda values: ", ".join(
                            sorted({str(value) for value in values if str(value).strip()})
                        ),
                    ),
                )
            )
            baseline_df = baseline_df.merge(
                observed_lookup,
                on=["feature_type", "feature_name"],
                how="left",
            )
            baseline_df["observed_usage_count"] = pd.to_numeric(
                baseline_df["observed_usage_count"],
                errors="coerce",
            ).fillna(0).astype(int)
            baseline_df["observed_in_profile"] = baseline_df["observed_usage_count"].gt(0)
        if not baseline_df.empty and not current_ruleset.override_df.empty:
            baseline_df = baseline_df.merge(
                current_ruleset.override_df.rename(
                    columns={
                        "enabled": "override_enabled",
                    }
                ),
                on=["feature_type", "feature_name"],
                how="left",
                suffixes=("", "_loaded"),
            )
            for target_col, source_col in [
                ("override_scope", "override_scope_loaded"),
                ("override_complexity", "override_complexity_loaded"),
                ("override_action", "override_action_loaded"),
                ("override_reason", "override_reason_loaded"),
                ("override_enabled", "override_enabled_loaded"),
            ]:
                if source_col in baseline_df.columns:
                    baseline_df[target_col] = baseline_df[source_col].where(
                        baseline_df[source_col].notna(),
                        baseline_df[target_col],
                    )
            drop_cols = [
                column
                for column in baseline_df.columns
                if column.endswith("_loaded")
            ]
            if drop_cols:
                baseline_df = baseline_df.drop(columns=drop_cols)
        usage_events_df = (
            st.session_state.mongo_usage_events_df.copy()
            if st.session_state.mongo_usage_events_df is not None
            else pd.DataFrame()
        )
        usage_metadata = st.session_state.mongo_usage_metadata or {}
        usage_metadata = {
            **usage_metadata,
            "rules_version": reassessed_result.rules_version,
            "override_count": reassessed_result.override_count,
            "rules_coverage_rate": reassessed_result.rules_coverage_rate,
            "unclassified_feature_count": reassessed_result.unclassified_feature_count,
        }
        usage_output_dir = st.session_state.mongo_usage_output_dir or ""

        usage_target_version, usage_target_mode = _render_oracle_target_controls(
            "usage_analysis_target_version",
            "usage_analysis_target_mode",
            usage_oracle_version_options,
            "按客户计划部署的 Oracle 版本和部署方式，评估 Oracle Database API for MongoDB 在该场景下的兼容程度。",
        )

        if "oracle_support_since" in usage_detail_df.columns:
            usage_detail_df["oracle_support_status"] = usage_detail_df["oracle_support_since"].map(
                lambda value: _effective_oracle_support_status(
                    value,
                    usage_target_version,
                    usage_target_mode,
                )
            )
        usage_summary_df = build_usage_summary(usage_detail_df)
        supported_usage_df = usage_detail_df[
            usage_detail_df["oracle_support_status"].eq("Supported")
        ].copy() if "oracle_support_status" in usage_detail_df.columns else pd.DataFrame()
        not_supported_usage_df = usage_detail_df[
            usage_detail_df["oracle_support_status"].eq("Not Supported")
        ].copy() if "oracle_support_status" in usage_detail_df.columns else pd.DataFrame()

        usage_overview_cols = st.columns(3, gap="medium")
        with usage_overview_cols[0]:
            st.metric("唯一 API 数量", _unique_api_count(usage_detail_df))
        with usage_overview_cols[1]:
            st.metric(
                "Supported",
                _unique_api_count(supported_usage_df),
            )
        with usage_overview_cols[2]:
            st.metric(
                "Not Supported",
                _unique_api_count(not_supported_usage_df),
            )

        if usage_metadata.get("truncated"):
            st.warning("本次查询命中了采样上限，结果可能被截断。建议缩小时间窗口或提高最大采样条数。")
        st.caption(
            f"数据库: {usage_metadata.get('database_name', '')} | "
            f"抓取时间: {usage_metadata.get('fetched_at', '')} | "
            f"开始时间: {usage_metadata.get('start_time', '') or '未设置'} | "
            f"结束时间: {usage_metadata.get('end_time', '') or '未设置'} | "
            f"规则版本: {usage_metadata.get('rules_version', '') or 'unknown'} | "
            f"客户覆盖: {int(usage_metadata.get('override_count', 0) or 0)} | "
            f"未分类 API: {int(usage_metadata.get('unclassified_feature_count', 0) or 0)} | "
            f"未知命令事件: {int(usage_metadata.get('unknown_command_event_count', 0) or 0)} | "
            f"未映射特征: {int(usage_metadata.get('unmapped_feature_count', 0) or 0)}"
        )

        selected_usage_status = [
            str(item) for item in st.session_state.get("usage_status_filter", []) or []
        ]
        selected_op_types = [
            str(item) for item in st.session_state.get("usage_op_filter", []) or []
        ]
        selected_feature_types = [
            str(item) for item in st.session_state.get("usage_feature_type_filter", []) or []
        ]
        selected_commands = [
            str(item) for item in st.session_state.get("usage_command_filter", []) or []
        ]
        selected_complexities = [
            str(item) for item in st.session_state.get("usage_complexity_filter", []) or []
        ]
        selected_scopes = [
            str(item) for item in st.session_state.get("usage_scope_filter", []) or []
        ]
        usage_keyword = str(st.session_state.get("usage_keyword_filter", "") or "")
        filtered_usage_df = _filter_usage_detail_df(
            usage_detail_df,
            selected_statuses=selected_usage_status,
            selected_op_types=selected_op_types,
            selected_feature_types=selected_feature_types,
            selected_commands=selected_commands,
            selected_complexities=selected_complexities,
            selected_scopes=selected_scopes,
            keyword=usage_keyword,
        )
        filtered_usage_summary_df = build_usage_summary(filtered_usage_df)
        filtered_migration_summary_df = (
            migration_summary_df.copy()
            if not migration_summary_df.empty
            else pd.DataFrame()
        )
        filtered_hotspots_df = (
            hotspots_df.copy()
            if not hotspots_df.empty
            else pd.DataFrame()
        )
        filtered_excluded_df = (
            excluded_df.copy()
            if not excluded_df.empty
            else pd.DataFrame()
        )

        usage_offline_report_html = _build_usage_offline_report_html(
            output_dir=usage_output_dir,
            metadata=usage_metadata,
            detail_df=usage_detail_df,
            baseline_df=baseline_df,
            source_detail_row_count=(
                len(st.session_state.result_detail_df)
                if st.session_state.result_detail_df is not None
                else 0
            ),
            target_version=usage_target_version,
            target_mode=usage_target_mode,
            selected_statuses=[
                str(item) for item in st.session_state.get("usage_status_filter", []) or []
            ],
            selected_op_types=[
                str(item) for item in st.session_state.get("usage_op_filter", []) or []
            ],
            selected_feature_types=[
                str(item) for item in st.session_state.get("usage_feature_type_filter", []) or []
            ],
            selected_commands=[
                str(item) for item in st.session_state.get("usage_command_filter", []) or []
            ],
            selected_complexities=[
                str(item) for item in st.session_state.get("usage_complexity_filter", []) or []
            ],
            selected_scopes=[
                str(item) for item in st.session_state.get("usage_scope_filter", []) or []
            ],
            keyword=str(st.session_state.get("usage_keyword_filter", "") or ""),
            show_baseline_advanced_columns=bool(
                st.session_state.get("usage_baseline_show_advanced_columns", False)
            ),
        )
        usage_workbook_bytes = _build_excel_workbook_bytes(
            [
                ("detail", filtered_usage_df),
                ("summary", filtered_usage_summary_df),
                ("migration_summary", filtered_migration_summary_df),
                ("hotspots", filtered_hotspots_df),
                ("excluded", filtered_excluded_df),
            ]
        )

        if st.session_state.mongo_usage_restored_from_disk and usage_output_dir:
            usage_last_refresh_time = datetime.fromtimestamp(Path(usage_output_dir).stat().st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            st.caption(f"上次刷新：{usage_last_refresh_time}")
        elif st.session_state.mongo_usage_cache_save_notice:
            st.caption(st.session_state.mongo_usage_cache_save_notice)
        else:
            st.caption("当前结果尚未写入本地缓存。")

        usage_download_cols = st.columns([0.65, 0.65, 0.65, 3.05], gap="medium")
        with usage_download_cols[0]:
            if st.button(
                "",
                key="save_usage_cache",
                help="保存当前结果到本地缓存",
                icon=":material/save:",
                width="stretch",
            ):
                saved_dir = _persist_usage_cache(
                    detail_df=usage_detail_df,
                    summary_df=usage_summary_df,
                    metadata=usage_metadata,
                    report_html=usage_offline_report_html,
                    workbook_bytes=usage_workbook_bytes,
                    migration_summary_df=migration_summary_df,
                    hotspots_df=hotspots_df,
                    excluded_df=excluded_df,
                )
                st.session_state.mongo_usage_output_dir = str(saved_dir)
                st.session_state.mongo_usage_restored_from_disk = False
                st.session_state.mongo_usage_cache_save_notice = f"已保存本地缓存：{saved_dir}"
                st.rerun()
        with usage_download_cols[1]:
            st.download_button(
                "",
                data=usage_workbook_bytes,
                file_name="mongodb_usage_analysis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="下载 Excel（detail + summary）",
                icon=":material/grid_on:",
                width="stretch",
            )
        with usage_download_cols[2]:
            st.download_button(
                "",
                data=usage_offline_report_html.encode("utf-8"),
                file_name="mongodb_usage_report.html",
                mime="text/html",
                help="下载 HTML 报告",
                icon=":material/language:",
                width="stretch",
            )
        with usage_download_cols[3]:
            st.markdown("")

        with st.container(border=True):
            st.markdown('<div class="panel-subsection-title">分析筛选</div>', unsafe_allow_html=True)
            usage_mode_label = _oracle_target_mode_label(usage_target_mode)
            st.caption(
                f"当前支持判断：Oracle {usage_target_version} + {usage_mode_label}"
            )
            usage_status_values = set(
                usage_detail_df["oracle_support_status"].fillna("").astype(str).unique().tolist()
            )
            if not baseline_df.empty and "oracle_support_statuses" in baseline_df.columns:
                for raw_value in baseline_df["oracle_support_statuses"].fillna("").astype(str).tolist():
                    usage_status_values.update(
                        item.strip() for item in raw_value.split(",") if item.strip()
                    )
            usage_status_options = sorted(usage_status_values)
            op_type_options = sorted(
                usage_detail_df["op_type"].fillna("").astype(str).unique().tolist()
            ) if "op_type" in usage_detail_df.columns else []
            feature_type_options = sorted(
                usage_detail_df["feature_type"].fillna("").astype(str).unique().tolist()
            )
            command_options = sorted(
                usage_detail_df["command_name"].fillna("").astype(str).unique().tolist()
            )
            complexity_options = sorted(
                usage_detail_df["effective_complexity"].fillna("").astype(str).unique().tolist()
            ) if "effective_complexity" in usage_detail_df.columns else []
            scope_values: set[str] = set()
            if "effective_migration_necessity" in usage_detail_df.columns:
                scope_values.update(
                    usage_detail_df["effective_migration_necessity"].fillna("").astype(str).tolist()
                )
            elif "effective_scope" in usage_detail_df.columns:
                scope_values.update(usage_detail_df["effective_scope"].fillna("").astype(str).tolist())
            if not baseline_df.empty:
                if "effective_migration_necessity" in baseline_df.columns:
                    scope_values.update(
                        baseline_df["effective_migration_necessity"].fillna("").astype(str).tolist()
                    )
                elif "effective_scope" in baseline_df.columns:
                    scope_values.update(baseline_df["effective_scope"].fillna("").astype(str).tolist())
            scope_options = sorted(option for option in scope_values if option)
            current_scope_state = [
                str(item)
                for item in st.session_state.get("usage_scope_filter", []) or []
                if str(item) in set(scope_options)
            ]
            if current_scope_state != (st.session_state.get("usage_scope_filter", []) or []):
                st.session_state.usage_scope_filter = current_scope_state

            primary_filter_cols = st.columns([1.4, 1, 1], gap="medium")
            with primary_filter_cols[0]:
                usage_keyword = st.text_input(
                    "关键字搜索",
                    key="usage_keyword_filter",
                    placeholder="搜索 feature / collection / op / Oracle 分类 / 迁移动作",
                )
            with primary_filter_cols[1]:
                selected_usage_status = st.multiselect(
                    "支持判断",
                    options=[option for option in usage_status_options if option],
                    key="usage_status_filter",
                    placeholder="支持判断",
                )
            with primary_filter_cols[2]:
                selected_feature_types = st.multiselect(
                    "功能类型",
                    options=[option for option in feature_type_options if option],
                    key="usage_feature_type_filter",
                    placeholder="功能类型",
                )

            with st.expander("更多筛选", expanded=False):
                advanced_filter_cols = st.columns(4, gap="medium")
                with advanced_filter_cols[0]:
                    selected_commands = st.multiselect(
                        "按命令筛选",
                        options=[option for option in command_options if option],
                        key="usage_command_filter",
                    )
                with advanced_filter_cols[1]:
                    selected_op_types = st.multiselect(
                        "按 Profile op 筛选",
                        options=[option for option in op_type_options if option],
                        key="usage_op_filter",
                    )
                with advanced_filter_cols[2]:
                    selected_complexities = st.multiselect(
                        "按迁移复杂度筛选",
                        options=[option for option in complexity_options if option],
                        key="usage_complexity_filter",
                    )
                with advanced_filter_cols[3]:
                    selected_scopes = st.multiselect(
                        "按迁移必要性筛选",
                        options=[option for option in scope_options if option],
                        key="usage_scope_filter",
                    )

            filtered_usage_df = _filter_usage_detail_df(
                usage_detail_df,
                selected_statuses=selected_usage_status,
                selected_op_types=selected_op_types,
                selected_feature_types=selected_feature_types,
                selected_commands=selected_commands,
                selected_complexities=selected_complexities,
                selected_scopes=selected_scopes,
                keyword=usage_keyword,
            )

        selected_risk_row = None
        risk_df = usage_detail_df[
            usage_detail_df["oracle_support_status"].isin(
                ["Partially Supported", "Not Supported", "Unknown"]
            )
        ].copy()
        if selected_op_types and "op_type" in risk_df.columns:
            risk_df = risk_df[risk_df["op_type"].isin(selected_op_types)]
        if selected_feature_types:
            risk_df = risk_df[risk_df["feature_type"].isin(selected_feature_types)]
        if selected_commands:
            risk_df = risk_df[risk_df["command_name"].isin(selected_commands)]
        if selected_complexities and "effective_complexity" in risk_df.columns:
            risk_df = risk_df[risk_df["effective_complexity"].isin(selected_complexities)]
        if selected_scopes:
            risk_necessity_col = (
                "effective_migration_necessity"
                if "effective_migration_necessity" in risk_df.columns
                else "effective_scope"
            )
            if risk_necessity_col in risk_df.columns:
                risk_df = risk_df[risk_df[risk_necessity_col].isin(selected_scopes)]
        if selected_usage_status:
            risk_df = risk_df[risk_df["oracle_support_status"].isin(selected_usage_status)]
        if usage_keyword.strip():
            keyword = usage_keyword.strip().lower()
            risk_search_cols = [
                "feature_name",
                "collection",
                "op_type",
                "command_name",
                "oracle_category",
                "oracle_support_since",
                "oracle_support_status",
                "effective_complexity",
                "effective_migration_necessity",
                "recommended_action",
            ]
            available_risk_search_cols = [col for col in risk_search_cols if col in risk_df.columns]
            risk_mask = risk_df[available_risk_search_cols].fillna("").astype(str).apply(
                lambda row: row.str.lower().str.contains(keyword, regex=False).any(),
                axis=1,
            )
            risk_df = risk_df[risk_mask]
        risk_df = risk_df.sort_values(
            by=["migration_priority", "effective_complexity", "usage_count", "feature_type", "feature_name"],
            ascending=[False, False, False, True, True],
        )

        usage_baseline_tab, usage_workload_tab = st.tabs(
            ["API 基准", "实际使用 API"]
        )

        with usage_baseline_tab:
            if baseline_df.empty:
                st.info("当前没有可展示的 API 基准。")
            else:
                if st.session_state.mongo_usage_override_save_notice:
                    st.success(st.session_state.mongo_usage_override_save_notice)
                baseline_source_df = _filter_baseline_df(
                    baseline_df,
                    selected_statuses=selected_usage_status,
                    selected_feature_types=selected_feature_types,
                    selected_commands=selected_commands,
                    selected_complexities=selected_complexities,
                    selected_scopes=selected_scopes,
                    keyword=usage_keyword,
                    only_observed=False,
                )
                total_api_count = _unique_api_count(baseline_source_df)
                observed_api_count = _unique_api_count(filtered_usage_df)
                source_detail_row_count = (
                    len(st.session_state.result_detail_df)
                    if st.session_state.result_detail_df is not None
                    else 0
                )
                baseline_metric_cols = st.columns(3, gap="medium")
                with baseline_metric_cols[0]:
                    st.metric("文档同步明细记录数", source_detail_row_count)
                with baseline_metric_cols[1]:
                    st.metric("全部唯一 API 数量", total_api_count)
                with baseline_metric_cols[2]:
                    st.metric("实际使用到的 API 数量", observed_api_count)

                st.caption(
                    "展示文档同步得到的全部 MongoDB API 的默认迁移必要性和迁移复杂度评估。"
                    "这里的必要性以应用兼容为核心：应用行为和应用管理对象保留在范围内，运维/诊断命令默认排除。"
                    "当前目标组合下只要 Oracle 标记为 Supported，默认认为应用代码可直接使用，复杂度降为 Low。"
                )
                show_baseline_advanced_columns = st.toggle(
                    "显示高级列",
                    key="usage_baseline_show_advanced_columns",
                    help="默认只显示迁移判断需要的核心列；开启后显示 Oracle 分类、原始支持状态、观察样本等辅助列。",
                )
                baseline_editor_df = baseline_source_df[
                    [
                        column
                        for column in _baseline_display_columns(show_baseline_advanced_columns)
                        if column in baseline_source_df.columns
                    ]
                ].copy()
                edited_baseline_df = st.data_editor(
                    baseline_editor_df,
                    width="stretch",
                    hide_index=True,
                    height=_dataframe_height(len(baseline_editor_df), max_height=520),
                    key="usage_baseline_editor",
                    disabled=[
                        "feature_type",
                        "feature_name",
                        "effective_migration_necessity",
                        "oracle_support_since",
                        "effective_complexity",
                        "complexity_explanation",
                    ],
                    column_config={
                        "mongo_short_description": st.column_config.TextColumn(
                            "mongo_short_description",
                            help="来自文档同步关联到的 MongoDB 官方说明。",
                            width="large",
                        ),
                        "observed_in_profile": st.column_config.CheckboxColumn(
                            "observed_in_profile",
                            help="该 API 是否在当前 system.profile 样本中被观察到。",
                        ),
                        "observed_usage_count": st.column_config.NumberColumn(
                            "observed_usage_count",
                            help="当前 profile 样本里聚合后的 API 观察次数。",
                            format="%d",
                        ),
                        "observed_command_contexts": st.column_config.TextColumn(
                            "observed_command_contexts",
                            help="该 API 在当前 profile 样本中出现的命令上下文。",
                            width="medium",
                        ),
                        "effective_migration_necessity": st.column_config.TextColumn(
                            "迁移必要性",
                            help="从应用兼容角度看该 API 是否有必要纳入迁移范围。",
                            width="medium",
                        ),
                        "complexity_explanation": st.column_config.TextColumn(
                            "complexity_explanation",
                            help="从迁移到 Oracle Database API for MongoDB 的角度解释该复杂度。",
                            width="large",
                        ),
                        "override_complexity": st.column_config.SelectboxColumn(
                            "override_complexity",
                            options=["", *sorted(ALLOWED_COMPLEXITY, key=lambda value: ["Ignore","Low","Medium","High","Blocker"].index(value))],
                            help="为空表示不覆盖默认复杂度。",
                        ),
                        "override_reason": st.column_config.TextColumn(
                            "override_reason",
                            help="填写覆盖原因，建议写项目上下文。",
                            width="large",
                        ),
                        "override_enabled": st.column_config.CheckboxColumn(
                            "override_enabled",
                            help="勾选后会把这一行写入 customer_overrides.csv。",
                        ),
                    },
                )
                baseline_action_cols = st.columns([0.9, 3.1], gap="medium")
                with baseline_action_cols[0]:
                    if st.button("保存覆盖规则", type="primary", width="stretch"):
                        override_rows = edited_baseline_df.copy()
                        if "override_enabled" not in override_rows.columns:
                            override_rows["override_enabled"] = False
                        for column in ["override_complexity", "override_reason"]:
                            override_rows[column] = override_rows[column].fillna("").astype(str).str.strip()
                        override_rows["override_enabled"] = override_rows["override_enabled"].fillna(False).astype(bool)
                        active_mask = (
                            override_rows["override_enabled"]
                            | override_rows["override_complexity"].ne("")
                            | override_rows["override_reason"].ne("")
                        )
                        overrides_to_save = override_rows.loc[
                            active_mask,
                            [
                                "feature_type",
                                "feature_name",
                                "override_complexity",
                                "override_reason",
                            ],
                        ].copy()
                        overrides_to_save["override_scope"] = ""
                        overrides_to_save["override_action"] = ""
                        overrides_to_save["enabled"] = "true"
                        save_path = save_customer_overrides(overrides_to_save)
                        st.session_state.mongo_usage_override_save_notice = f"已保存覆盖规则：{save_path}"
                        st.rerun()
                with baseline_action_cols[1]:
                    st.caption("保存后会写入 `config/migration_rules/customer_overrides.csv`，并在当前页面自动重算迁移复杂度。")

        with usage_workload_tab:
            if filtered_usage_df.empty:
                st.info("当前没有可展示的实际使用 API。")
            else:
                workload_summary_cols = st.columns(3, gap="medium")
                with workload_summary_cols[0]:
                    st.metric("实际使用 API 数量", _unique_api_count(filtered_usage_df))
                with workload_summary_cols[1]:
                    st.metric(
                        "高复杂度 API",
                        _unique_api_count(
                            filtered_usage_df[
                                filtered_usage_df["effective_complexity"].isin(["High", "Blocker"])
                            ]
                        ) if "effective_complexity" in filtered_usage_df.columns else 0,
                    )
                with workload_summary_cols[2]:
                    filtered_hotspots_df = filtered_usage_df[
                        filtered_usage_df["effective_scope"].eq("application_api")
                        & (
                            filtered_usage_df["effective_complexity"].isin(["High", "Blocker"])
                            | (
                                filtered_usage_df["effective_complexity"].eq("Medium")
                                & pd.to_numeric(
                                    filtered_usage_df["usage_count"],
                                    errors="coerce",
                                ).fillna(0).ge(20)
                            )
                        )
                    ].copy() if {
                        "effective_scope",
                        "effective_complexity",
                        "usage_count",
                    }.issubset(filtered_usage_df.columns) else pd.DataFrame()
                    st.metric("热点项数量", _unique_api_count(filtered_hotspots_df))

                workload_display_df = filtered_usage_df[
                    [
                        column for column in [
                            "feature_type",
                            "feature_name",
                            "command_name",
                            "effective_migration_necessity",
                            "usage_count",
                            "oracle_support_status",
                            "effective_complexity",
                            "migration_priority",
                        ] if column in filtered_usage_df.columns
                    ]
                ].copy()
                st.caption("展示当前 system.profile 中实际观察到的 API，并同时给出迁移必要性、复杂度和建议动作。当前目标组合下只要 Oracle 标记为 Supported，默认复杂度降为 Low。")
                selected_risk_event = st.dataframe(
                    workload_display_df,
                    width="stretch",
                    hide_index=True,
                    height=_dataframe_height(len(workload_display_df), max_height=420),
                    key="usage_risk_selector_table",
                    on_select="rerun",
                    selection_mode="single-row",
                    column_config=_usage_column_config(),
                )
                selected_risk_indices = _selected_dataframe_rows(selected_risk_event)
                if selected_risk_indices:
                    selected_risk_row = filtered_usage_df.iloc[selected_risk_indices[0]]
                st.markdown("#### 证据样本")
                if selected_risk_row is None:
                    st.info("请先在上面的“实际使用 API”表格中选择一行，再查看证据样本。")
                else:
                    evidence_mask = (
                        filtered_usage_df["feature_type"].astype(str).eq(str(selected_risk_row["feature_type"]))
                        & filtered_usage_df["feature_name"].astype(str).eq(str(selected_risk_row["feature_name"]))
                        & filtered_usage_df["command_name"].astype(str).eq(str(selected_risk_row["command_name"]))
                        & filtered_usage_df["database"].astype(str).eq(str(selected_risk_row["database"]))
                        & filtered_usage_df["collection"].astype(str).eq(str(selected_risk_row["collection"]))
                        & filtered_usage_df["oracle_support_status"].astype(str).eq(
                            str(selected_risk_row["oracle_support_status"])
                        )
                    )
                    if "op_type" in filtered_usage_df.columns:
                        evidence_mask = evidence_mask & filtered_usage_df["op_type"].fillna("").astype(str).eq(
                            str(selected_risk_row.get("op_type", "") or "")
                        )
                    evidence_df = filtered_usage_df[evidence_mask].copy().sort_values(
                        by=["usage_count", "feature_type", "feature_name"],
                        ascending=[False, True, True],
                    )
                    if evidence_df.empty:
                        st.info("当前选中项没有可展示的证据样本。")
                    else:
                        evidence_row = evidence_df.iloc[0]
                        evidence_cols = st.columns(2, gap="large")
                        with evidence_cols[0]:
                            st.markdown(
                                f"""
                                - `feature_type`: {evidence_row['feature_type']}
                                - `feature_name`: {evidence_row['feature_name']}
                                - `command_name`: {evidence_row['command_name']}
                                - `op_type`: {evidence_row.get('op_type', '') or '-'}
                                - `database.collection`: {evidence_row['database']}.{evidence_row['collection']}
                                - `usage_count`: {evidence_row['usage_count']}
                                - `oracle_support_status`: {evidence_row['oracle_support_status']}
                                - `oracle_support_since`: {evidence_row['oracle_support_since'] or '-'}
                                - `oracle_category`: {evidence_row['oracle_category'] or '-'}
                                - `effective_migration_necessity`: {evidence_row.get('effective_migration_necessity', '') or '-'}
                                - `effective_complexity`: {evidence_row.get('effective_complexity', '') or '-'}
                                - `recommended_action`: {evidence_row.get('recommended_action', '') or '-'}
                                """
                            )
                        with evidence_cols[1]:
                            st.markdown(f"**命令中的位置**: `{evidence_row['sample_path']}`")
                            st.code(str(evidence_row["sample_value"]), language="json")
