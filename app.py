from __future__ import annotations

import altair as alt
import html as html_lib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from src.oracle_feature_support.fetcher import INDEX_URL, TARGET_URL, analyze_feature_support
from src.oracle_feature_support.mongodb_reference import (
    ABOUT_URL,
    MONGODB_REFERENCE_SOURCES,
    enrich_feature_support_detail,
    load_mongodb_reference_catalog,
    load_mongodb_reference_metadata,
    sync_mongodb_reference_catalog,
)

SETTINGS_PATH = Path("outputs/.ui_settings.json")

st.set_page_config(page_title="Oracle MongoDB API Feature Support", layout="wide")
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2.5rem;
        max-width: 1280px;
    }
    .app-hero {
        background: linear-gradient(135deg, #0f3b45 0%, #176b72 56%, #2f8f83 100%);
        border-radius: 8px;
        color: #ffffff;
        padding: 1.4rem 1.6rem;
        margin-bottom: 1rem;
    }
    .app-hero h1 {
        font-size: 2rem;
        line-height: 1.2;
        margin: 0 0 0.35rem 0;
    }
    .app-hero p {
        color: #d9f2ee;
        margin: 0;
    }
    div[data-testid="stExpander"] {
        border: 1px solid #dde5e7;
        border-radius: 10px;
        background: #fbfcfc;
    }
    div[data-testid="stExpander"] details summary p {
        font-weight: 700;
    }
    .config-sidecard {
        border: 1px solid #d7e4e2;
        border-radius: 10px;
        background: linear-gradient(180deg, #f7fbfb 0%, #f2f7f7 100%);
        padding: 0.9rem 1rem;
        min-height: 100%;
    }
    .config-sidecard h4 {
        margin: 0 0 0.5rem 0;
        font-size: 1rem;
        color: #0f3b45;
    }
    .config-sidecard p {
        margin: 0 0 0.45rem 0;
        color: #4c6268;
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
        color: #1f2937;
    }
    .icon-toolbar-actions {
        display: flex;
        align-items: center;
        gap: 0.4rem;
    }
    div[data-testid="stMetric"] {
        background: #f6f8fa;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        padding: 0.85rem 1rem;
    }
    .stButton > button,
    .stFormSubmitButton > button {
        min-height: 2.6rem;
        border-radius: 10px;
        border: 1px solid #cfe0de;
        background: #f5f9f9;
        color: #0f3b45;
        font-weight: 700;
        white-space: nowrap;
        transition: background 0.18s ease, border-color 0.18s ease, color 0.18s ease, box-shadow 0.18s ease;
    }
    .stButton > button:hover,
    .stFormSubmitButton > button:hover {
        background: linear-gradient(135deg, #176b72 0%, #2f8f83 100%);
        border-color: #176b72;
        color: #ffffff;
        box-shadow: 0 8px 18px rgba(23, 107, 114, 0.18);
    }
    .stButton > button[kind="primary"],
    .stFormSubmitButton > button[kind="primary"] {
        background: linear-gradient(135deg, #0f3b45 0%, #176b72 100%);
        color: #ffffff;
        border-color: #0f3b45;
    }
    .stButton > button[kind="primary"]:hover,
    .stFormSubmitButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #0b3139 0%, #135961 100%);
        color: #ffffff;
    }
    div[data-testid="stDataFrame"] {
        border-radius: 8px;
    }
    </style>
    <div class="app-hero">
        <h1>Oracle MongoDB API Feature Support 分析台</h1>
        <p>手工触发同步，跟踪 Oracle 与 MongoDB 文档版本，分析 MongoDB API 支持情况。</p>
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
if "settings_loaded" not in st.session_state:
    st.session_state.settings_loaded = False
if "detail_expanded_sections" not in st.session_state:
    st.session_state.detail_expanded_sections = {}


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


with st.container(border=True):
    st.markdown("#### 同步配置")
    st.markdown("##### 数据源配置")
    st.caption("文档链接与来源中的链接修改后会自动保存并立即生效。")
    if "doc_links_expanded" not in st.session_state:
        st.session_state.doc_links_expanded = False
    header_cols = st.columns([7.3, 0.55, 0.55, 0.55], gap="small")
    with header_cols[0]:
        st.markdown('<div class="icon-toolbar-title" style="padding-top:0.35rem;">文档链接与来源</div>', unsafe_allow_html=True)
    with header_cols[1]:
        save_url = st.button("★", key="save_doc_links_default", help="将当前链接保存为默认 URL", use_container_width=True)
    with header_cols[2]:
        restore_default_url = st.button("↺", key="restore_doc_links_default", help="恢复默认 URL", use_container_width=True)
    with header_cols[3]:
        toggle_doc_links = st.button(
            "▾" if st.session_state.doc_links_expanded else "▸",
            key="toggle_doc_links",
            help="展开或折叠文档链接与来源",
            use_container_width=True,
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
            source_df = pd.DataFrame(source_rows)
            source_df["上次抓取数"] = source_df["上次抓取数"].map(
                lambda value: str(int(value)) if str(value).strip() not in {"", "None"} else "-"
            )
            edited_source_df = st.data_editor(
                source_df[["文档项", "链接", "上次抓取数", "用途说明"]],
                key="doc_link_editor",
                use_container_width=True,
                hide_index=True,
                height=_dataframe_height(len(source_df), max_height=300),
                disabled=["文档项", "上次抓取数", "用途说明"],
                column_config={
                    "文档项": st.column_config.TextColumn(width="medium"),
                    "链接": st.column_config.TextColumn(width="large"),
                    "上次抓取数": st.column_config.TextColumn(width="small"),
                    "用途说明": st.column_config.TextColumn(width="medium"),
                },
            )
        else:
            edited_source_df = None
            st.caption("暂无文档链接。")
        st.markdown(
            """
            <div class="config-sidecard">
              <h4>配置说明</h4>
              <p>编辑链接列后会自动保存并立即生效。</p>
              <p>Oracle 链接用于同步 Feature Support 内容。</p>
              <p>MongoDB 文档版本页用于版本确认与说明同步。</p>
              <p>“✓” 会将当前链接保存为默认值；“↺” 会恢复默认值。</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.caption("Oracle 与 MongoDB 链接已统一合并到“文档链接与来源”中展示。")
    else:
        edited_source_df = None

    st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)
    st.markdown("##### 同步执行")
    with st.form("run_form", clear_on_submit=False):
        form_cols = st.columns([1, 1, 1.2], gap="large")
        with form_cols[0]:
            timeout = st.number_input(
                "请求超时（秒）", min_value=10, max_value=180, step=5, key="ui_timeout"
            )
        with form_cols[1]:
            max_retries = st.number_input(
                "重试次数", min_value=0, max_value=6, step=1, key="ui_max_retries"
            )
        with form_cols[2]:
            show_debug_log = st.checkbox("显示执行日志（排查问题）", key="ui_show_debug")
        st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)
        action_cols = st.columns([1.45, 1.45, 2.6], gap="medium")
        with action_cols[0]:
            sync_reference = st.form_submit_button("同步 MongoDB 官方说明", use_container_width=True)
        with action_cols[1]:
            submitted = st.form_submit_button("同步 Oracle 官方文档", type="primary", use_container_width=True)
        with action_cols[2]:
            st.markdown("")
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
              <p>同步 Oracle 官方文档会更新 Oracle 最新 Feature Support，并结合本地 MongoDB 官方说明展示结果。</p>
              <p>说明缓存：{cache_summary}</p>
              <p>MongoDB 官方说明仅在点击“同步 MongoDB 官方说明”时更新；同步 Oracle 官方文档不会自动更新说明缓存。</p>
              <p>勾选执行日志后，MongoDB 官方说明同步过程也会输出详细日志。</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

if edited_source_df is not None:
    updated_doc_link_urls = st.session_state.ui_doc_link_urls.copy()
    changed = False
    for definition, (_, row) in zip(_document_link_definitions(), edited_source_df.iterrows()):
        new_value = str(row["链接"]).strip() or definition["default_url"]
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
    st.session_state.pop("doc_link_editor", None)
    _save_ui_settings()
    st.rerun()

log_container = st.empty()


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
        "<div style='width:100%; padding-top:0.15rem;'>"
        "<div style='display:flex;width:100%;height:12px;border-radius:999px;background:#e7eef0;overflow:hidden;'>"
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
    selected_support_values: list[str],
    only_with_description: bool,
) -> pd.DataFrame:
    filtered = df.copy()

    if selected_sections and "section" in filtered.columns:
        filtered = filtered[filtered["section"].astype(str).isin(selected_sections)]

    if selected_support_values and "Support (Since)" in filtered.columns:
        filtered = filtered[filtered["Support (Since)"].astype(str).isin(selected_support_values)]

    if only_with_description and "mongo_short_description" in filtered.columns:
        filtered = filtered[
            filtered["mongo_short_description"].fillna("").astype(str).str.strip().ne("")
        ]

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
    prepared = prepared.drop(columns=["normalized_status"], errors="ignore")
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
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        "detailColumns": detail_columns,
        "detailRows": detail_rows,
        "documentLinks": document_link_df.fillna("").astype(str).to_dict(orient="records"),
        "summaryTables": {
            "status": status_summary_df.fillna("").astype(str).to_dict(orient="records"),
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
  <title>Oracle MongoDB API Feature Support Report</title>
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
      max-width: 1280px;
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
      display: block;
      width: 100%;
      height: 12px;
      border-radius: 999px;
      background: #e7eef0;
      overflow: hidden;
    }}
    .section-summary-bar-fill {{
      display: block;
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
      <h1>Oracle MongoDB API - Feature Support</h1>
      <div>离线 HTML 报告</div>
      <div class="note">生成时间: {generated_at} | 单文件离线交互快照</div>
    </div>

    <section class="panel">
      <h2>结果总览</h2>
      <div class="meta-grid">
        <div class="meta-card"><div class="label">明细记录数</div><div class="value">{len(filtered_detail_df)}</div></div>
        <div class="meta-card"><div class="label">Oracle 文档版本时间</div><div class="value">{html_lib.escape(doc_metadata.get('doc_version_date', '') or '未解析到')}</div></div>
        <div class="meta-card"><div class="label">Oracle 文档编号</div><div class="value">{html_lib.escape(doc_metadata.get('doc_id', '') or '未解析到')}</div></div>
        <div class="meta-card"><div class="label">MongoDB 手册版本</div><div class="value">{html_lib.escape(str(reference_metadata.get('mongodb_manual_version', '')) or '未同步')}</div></div>
      </div>
      <div class="version-lines">
        <div class="note">导出时间: {generated_at}</div>
        <div class="note">结果目录: {html_lib.escape(output_dir)}</div>
        <div class="note">MongoDB 官方说明同步时间: {html_lib.escape(str(reference_metadata.get('synced_at', '未知')))} | 新增 {html_lib.escape(str(reference_metadata.get('new_entry_count', 0)))} | 更新 {html_lib.escape(str(reference_metadata.get('updated_entry_count', 0)))} | 说明已覆盖 {html_lib.escape(str(payload["mongodbDescriptionCoverage"]))}</div>
        <div class="note">Oracle 版本判断: {html_lib.escape(doc_metadata.get('update_status', ''))}</div>
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
        <h2>API 支持汇总</h2>
        <div id="status-chart" class="chart-block"></div>
        <div id="status-table" class="table-wrap"></div>
      </section>
      <section class="panel">
      <h2>Oracle 起始支持版本</h2>
        <div id="support-chart" class="chart-block"></div>
        <div id="support-table" class="table-wrap"></div>
      </section>
    </div>

    <section class="panel">
      <h2>Feature Support 明细分析</h2>
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
          <label>按 Support (Since) 筛选</label>
          <div id="support-filter" class="multi-select"></div>
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
    const sectionFilter = document.getElementById("section-filter");
    const supportFilter = document.getElementById("support-filter");
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

    function renderBars(targetId, title, items, colorResolver) {{
      const target = document.getElementById(targetId);
      if (!items.length) {{
        target.innerHTML = `<h3>${{escapeHtml(title)}}</h3><p class="empty-state">暂无数据</p>`;
        return;
      }}
      const maxCount = Math.max(...items.map((item) => item.count), 1);
      const rowsHtml = items.map((item, index) => {{
        const color = colorResolver(item, index);
        const width = (item.count / maxCount) * 100;
        return `
          <div class="bar-row" title="${{escapeHtml(item.label)}}: ${{item.count}} (${{item.percentage}}%)">
            <div class="bar-label">${{escapeHtml(item.label)}}</div>
            <div class="bar-track"><div class="bar-fill" style="width:${{width}}%;background:${{color}};"></div></div>
            <div class="bar-value">${{item.count}} / ${{item.percentage}}%</div>
          </div>
        `;
      }}).join("");
      target.innerHTML = `<h3>${{escapeHtml(title)}}</h3><div class="bars">${{rowsHtml}}</div>`;
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

    function populateSelect(container, values, placeholder) {{
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
        input.addEventListener("change", () => {{
          updateMultiSelectButton(container);
          applyFilters();
        }});
      }});

      updateMultiSelectButton(container);
    }}

    function buildFilterText(row) {{
      return reportData.detailColumns
        .map((column) => String(row[column] || ""))
        .join(" ")
        .toLowerCase();
    }}

    function applyFilters() {{
      const keyword = keywordFilter.value.trim().toLowerCase();
      const selectedSections = new Set(selectedValues(sectionFilter));
      const selectedSupport = new Set(selectedValues(supportFilter));
      const onlyWithDescription = descOnlyFilter.checked;

      const filteredRows = reportData.detailRows.filter((row) => {{
        if (selectedSections.size && !selectedSections.has(row.__section || "")) {{
          return false;
        }}
        if (selectedSupport.size && !selectedSupport.has(row.__support_since || "")) {{
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

      renderSummaries(filteredRows);
      renderDetails(filteredRows);
    }}

    function renderSummaries(rows) {{
      const statusRows = aggregateCounts(rows, "__status", "Other");
      const supportRows = aggregateCounts(rows, "__support_since", "Unknown").slice(0, 20);
      const sectionRows = aggregateCounts(rows, "__section", "Unknown Section");

      renderBars(
        "status-chart",
        "API 支持汇总",
        statusRows,
        (item) => statusColorMap[item.label] || statusColorMap.Other
      );
      renderBars(
        "support-chart",
        "Oracle 起始支持版本",
        supportRows,
        (_, index) => palette[index % palette.length]
      );
      document.getElementById("status-table").innerHTML = createTable(
        ["normalized_status", "count", "percentage"],
        statusRows.map((item) => ({{
          normalized_status: item.label,
          count: item.count,
          percentage: `${{item.percentage}}%`,
        }}))
      );
      document.getElementById("support-table").innerHTML = createTable(
        ["support_since", "count", "percentage"],
        supportRows.map((item) => ({{
          support_since: item.label,
          count: item.count,
          percentage: `${{item.percentage}}%`,
        }}))
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
      supportFilter.querySelectorAll("input[type='checkbox']").forEach((input) => {{
        input.checked = false;
      }});
      updateMultiSelectButton(sectionFilter);
      updateMultiSelectButton(supportFilter);
      descOnlyFilter.checked = false;
      applyFilters();
    }}

    function init() {{
      populateSelect(sectionFilter, uniqueSorted(reportData.detailRows.map((row) => row.__section || "Unknown Section")), "选择 section");
      populateSelect(supportFilter, uniqueSorted(reportData.detailRows.map((row) => row.__support_since || "Unknown")), "选择 Support (Since)");

      keywordFilter.addEventListener("input", applyFilters);
      descOnlyFilter.addEventListener("change", applyFilters);
      clearFiltersButton.addEventListener("click", clearFilters);
      expandAllButton.addEventListener("click", () => expandOrCollapseAll(true));
      collapseAllButton.addEventListener("click", () => expandOrCollapseAll(false));
      document.addEventListener("click", (event) => {{
        if (!sectionFilter.contains(event.target)) {{
          sectionFilter.classList.remove("open");
        }}
        if (!supportFilter.contains(event.target)) {{
          supportFilter.classList.remove("open");
        }}
      }});

      applyFilters();
    }}

    init();
  </script>
</body>
</html>
"""


def _latest_output_dir(output_root: str = "outputs") -> Path | None:
    root = Path(output_root)
    if not root.exists():
        return None
    candidates = [p for p in root.glob("feature_support_*") if p.is_dir()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _restore_last_result() -> bool:
    latest_dir = _latest_output_dir()
    if latest_dir is None:
        return False

    detail_path = latest_dir / "feature_support_detail.csv"
    summary_path = latest_dir / "feature_support_summary.csv"
    metadata_path = latest_dir / "document_metadata.json"
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
    st.session_state.result_output_dir = str(latest_dir)
    st.session_state.doc_metadata = doc_metadata
    return True


if (
    not st.session_state.restore_attempted
    and st.session_state.result_detail_df is None
    and st.session_state.result_summary_df is None
):
    st.session_state.restore_attempted = True
    st.session_state.restored_from_disk = _restore_last_result()


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
            st.success(
                "MongoDB 官方说明同步完成，"
                f"共 {sync_result.metadata.get('entry_count', 0)} 条。"
            )

if submitted:
    _save_ui_settings()
    st.session_state.debug_logs = []
    st.session_state.show_debug_log_runtime = show_debug_log
    st.session_state.restored_from_disk = False
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
            st.info("建议先把“请求超时（秒）”调到 90-120，再重试一次。")
        else:
            st.session_state.result_detail_df = result.detail_df
            st.session_state.result_summary_df = result.summary_df
            st.session_state.result_output_dir = str(result.output_dir)
            st.session_state.doc_metadata = result.doc_metadata

if st.session_state.get("show_debug_log_runtime", True) and st.session_state.debug_logs:
    log_container.empty()
    with log_container.container():
        with st.expander("▸ 执行日志", expanded=True):
            st.code("\n".join(st.session_state.debug_logs[-200:]), language="text")

if st.session_state.result_detail_df is not None and st.session_state.result_summary_df is not None:
    detail_df = st.session_state.result_detail_df
    summary_df = st.session_state.result_summary_df
    output_dir = st.session_state.result_output_dir
    doc_metadata = st.session_state.doc_metadata or {}
    reference_df = st.session_state.reference_df
    reference_metadata = st.session_state.reference_metadata or {}
    has_metric = "metric" in summary_df.columns
    enriched_detail_df = (
        enrich_feature_support_detail(detail_df, reference_df)
        if not reference_df.empty
        else detail_df.copy()
    )
    description_match_count = (
        enriched_detail_df["mongo_short_description"].fillna("").astype(str).str.strip().ne("").sum()
        if "mongo_short_description" in enriched_detail_df.columns
        else 0
    )

    status_summary_df_raw = (
        summary_df[summary_df["metric"] == "normalized_status"].copy()
        if has_metric
        else summary_df.copy()
    )
    status_summary_df = status_summary_df_raw[["normalized_status", "count", "percentage"]]

    support_top_display_df = pd.DataFrame()
    section_display_df = pd.DataFrame()
    if has_metric:
        support_top_df = summary_df[summary_df["metric"] == "support_value_top20"].copy()
        if not support_top_df.empty:
            support_top_display_df = support_top_df.rename(
                columns={"normalized_status": "support_since"}
            )
            support_top_display_df = support_top_display_df[
                ["support_since", "count", "percentage"]
            ]

        section_df = summary_df[summary_df["metric"] == "section_count"].copy()
        if not section_df.empty:
            section_display_df = section_df.rename(columns={"normalized_status": "section"})
            section_display_df = section_display_df[["section", "count", "percentage"]]

    offline_report_html = _build_offline_report_html(
        output_dir=output_dir,
        doc_metadata=doc_metadata,
        reference_metadata=reference_metadata,
        status_summary_df=status_summary_df,
        support_top_display_df=support_top_display_df,
        section_display_df=section_display_df,
        filtered_detail_df=enriched_detail_df,
    )
    offline_report_path = Path(output_dir) / "feature_support_report.html"
    offline_report_path.write_text(offline_report_html, encoding="utf-8")

    st.markdown("### 结果总览")
    overview_cols = st.columns([1.15, 1.15, 1.15, 1.05])
    with overview_cols[0]:
        st.metric("明细记录数", len(enriched_detail_df))
    with overview_cols[1]:
        st.metric("Oracle 文档版本时间", doc_metadata.get("doc_version_date", "") or "未解析到")
    with overview_cols[2]:
        st.metric("Oracle 文档编号", doc_metadata.get("doc_id", "") or "未解析到")
    with overview_cols[3]:
        st.metric(
            "MongoDB 手册版本",
            str(reference_metadata.get("mongodb_manual_version", "")) or "未同步",
        )

    if st.session_state.restored_from_disk:
        last_refresh_time = datetime.fromtimestamp(Path(output_dir).stat().st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        st.info(f"上次刷新时间: {last_refresh_time}")
    else:
        st.success(f"Oracle 官方文档同步完成，结果已保存到: {output_dir}")

    if doc_metadata:
        update_status = doc_metadata.get("update_status", "")
        if "发现文档版本变化" in update_status:
            st.warning(update_status)
        elif update_status:
            st.info(update_status)

    if reference_metadata:
        st.caption(
            "MongoDB 官方说明同步时间: "
            f"{reference_metadata.get('synced_at', '未知')} "
            f"| MongoDB 手册版本 {reference_metadata.get('mongodb_manual_version', '未知')} "
            f"| 新增 {reference_metadata.get('new_entry_count', 0)} "
            f"| 更新 {reference_metadata.get('updated_entry_count', 0)} "
            f"| 说明已覆盖 {description_match_count}"
        )

    download_cols = st.columns([1, 1, 1, 3])
    with download_cols[0]:
        st.download_button(
            "下载明细 CSV",
            data=enriched_detail_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="feature_support_detail.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with download_cols[1]:
        st.download_button(
            "下载统计 CSV",
            data=summary_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="feature_support_summary.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with download_cols[2]:
        st.download_button(
            "下载 HTML 报告",
            data=offline_report_html.encode("utf-8"),
            file_name="feature_support_report.html",
            mime="text/html",
            use_container_width=True,
        )
    with download_cols[3]:
        st.caption(f"离线 HTML 已保存到: {offline_report_path}")

    st.markdown("### 支持情况分析")
    chart_cols = st.columns([1.05, 0.95])

    with chart_cols[0]:
        with st.container(border=True):
            st.subheader("API 支持汇总")
            st.dataframe(status_summary_df, use_container_width=True, height=220)
            status_color_domain = ["Supported", "Not Supported", "Partially Supported", "Other"]
            status_color_range = ["#2E8B57", "#D9534F", "#F0AD4E", "#7A7A7A"]
            status_chart = (
                alt.Chart(status_summary_df)
                .mark_bar(cornerRadiusEnd=3)
                .encode(
                    x=alt.X("normalized_status:N", title="Status"),
                    y=alt.Y("count:Q", title="Count"),
                    color=alt.Color(
                        "normalized_status:N",
                        scale=alt.Scale(domain=status_color_domain, range=status_color_range),
                        legend=None,
                    ),
                    tooltip=["normalized_status", "count", "percentage"],
                )
            )
            st.altair_chart(status_chart, use_container_width=True)

    with chart_cols[1]:
        with st.container(border=True):
            st.subheader("Oracle 起始支持版本")
            if support_top_display_df.empty:
                st.info("没有可展示的版本统计。")
            else:
                pie_data = support_top_display_df.copy()
                pie_data["support_since"] = pie_data["support_since"].astype(str)
                pie_chart = (
                    alt.Chart(pie_data)
                    .mark_arc(innerRadius=55)
                    .encode(
                        theta=alt.Theta(field="count", type="quantitative"),
                        color=alt.Color(
                            field="support_since",
                            type="nominal",
                            title="Support Since",
                            scale=alt.Scale(scheme="tableau20"),
                        ),
                        tooltip=["support_since", "count", "percentage"],
                    )
                )
                st.altair_chart(pie_chart, use_container_width=True)
            if not support_top_display_df.empty:
                st.dataframe(support_top_display_df, use_container_width=True, height=220)

    with st.container(border=True):
        st.markdown('<div id="feature-support-detail"></div>', unsafe_allow_html=True)
        st.subheader("Feature Support 明细分析")

        filter_cols = st.columns([1.4, 1.2, 1.2, 0.8])
        with filter_cols[0]:
            detail_keyword = st.text_input(
                "关键字搜索",
                placeholder="搜索 Command / Operator / Stage / 功能说明",
                key="detail_keyword",
            )
        with filter_cols[1]:
            section_options = (
                sorted(enriched_detail_df["section"].dropna().astype(str).unique().tolist())
                if "section" in enriched_detail_df.columns
                else []
            )
            selected_sections = st.multiselect(
                "按 section 筛选",
                options=section_options,
                key="detail_sections",
            )
        with filter_cols[2]:
            support_options = (
                sorted(enriched_detail_df["Support (Since)"].dropna().astype(str).unique().tolist())
                if "Support (Since)" in enriched_detail_df.columns
                else []
            )
            selected_support_values = st.multiselect(
                "按 Support (Since) 筛选",
                options=support_options,
                key="detail_support_values",
            )
        with filter_cols[3]:
            only_with_description = st.checkbox(
                "仅看有说明",
                key="detail_only_with_description",
            )

        filtered_detail_df = _filter_detail_df(
            enriched_detail_df,
            keyword=detail_keyword,
            selected_sections=selected_sections,
            selected_support_values=selected_support_values,
            only_with_description=only_with_description,
        )
        offline_report_html = _build_offline_report_html(
            output_dir=output_dir,
            doc_metadata=doc_metadata,
            reference_metadata=reference_metadata,
            status_summary_df=status_summary_df,
            support_top_display_df=support_top_display_df,
            section_display_df=section_display_df,
            filtered_detail_df=filtered_detail_df,
        )
        offline_report_path = Path(output_dir) / "feature_support_report.html"
        offline_report_path.write_text(offline_report_html, encoding="utf-8")

        if "section" in filtered_detail_df.columns:
            grouped = list(filtered_detail_df.groupby("section", dropna=False, sort=False))
            for section_name, sec_df in grouped:
                section_text = str(section_name) if str(section_name).strip() else "Unknown Section"
                expanded = bool(st.session_state.detail_expanded_sections.get(section_text, False))
                with st.container(border=True):
                    row_cols = st.columns([2.5, 4.7, 0.8])
                    with row_cols[0]:
                        st.markdown(f"**{section_text} ({len(sec_df)} 条)**")
                    with row_cols[1]:
                        st.markdown(
                            _section_status_bar_html(sec_df),
                            unsafe_allow_html=True,
                        )
                    with row_cols[2]:
                        if st.button("▾" if expanded else "▸", key=f"toggle_section_{section_text}", use_container_width=True):
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
                        use_container_width=True,
                        height=_dataframe_height(len(sec_display_df)),
                        hide_index=True,
                    )
        else:
            detail_display_df = _prepare_detail_display_df(
                filtered_detail_df,
                include_section=False,
            )
            st.dataframe(
                detail_display_df,
                use_container_width=True,
                height=_dataframe_height(len(detail_display_df), max_height=520),
                hide_index=True,
            )
