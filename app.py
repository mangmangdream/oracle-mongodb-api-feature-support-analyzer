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
    div[data-testid="stMetric"] {
        background: #f6f8fa;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        padding: 0.85rem 1rem;
    }
    div[data-testid="stDataFrame"] {
        border-radius: 8px;
    }
    </style>
    <div class="app-hero">
        <h1>Oracle MongoDB API - Feature Support</h1>
        <p>手工触发抓取，跟踪 Oracle 文档版本，汇总 MongoDB API 支持情况。</p>
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


def _save_ui_settings() -> None:
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(
        json.dumps(
            {
                "url": st.session_state.ui_saved_url,
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
    saved_url = saved_settings.get("url", TARGET_URL)
    st.session_state.ui_saved_url = saved_url
    st.session_state.ui_url_draft = saved_url
    st.session_state.ui_timeout = int(saved_settings.get("timeout", 60))
    st.session_state.ui_max_retries = int(saved_settings.get("max_retries", 3))
    st.session_state.ui_show_debug = bool(saved_settings.get("show_debug_log", True))
    st.session_state.settings_loaded = True

if st.session_state.reference_df.empty:
    st.session_state.reference_df = load_mongodb_reference_catalog()
if not st.session_state.reference_metadata:
    st.session_state.reference_metadata = load_mongodb_reference_metadata()

with st.container(border=True):
    st.markdown("#### 目标链接")
    with st.form("url_form", clear_on_submit=False):
        url_draft = st.text_input("当前 Oracle 文档链接", value=st.session_state.ui_url_draft)
        st.caption("URL 修改后需要先保存；开始抓取并分析和同步 MongoDB 官方说明只会使用已保存的 URL。")
        url_action_cols = st.columns([0.45, 0.45, 0.45, 4.65])
        with url_action_cols[0]:
            save_url = st.form_submit_button("✓", help="保存当前 URL", use_container_width=True)
        with url_action_cols[1]:
            cancel_url = st.form_submit_button("×", help="取消未保存的修改", use_container_width=True)
        with url_action_cols[2]:
            restore_default_url = st.form_submit_button("↺", help="恢复默认 URL", use_container_width=True)
        with url_action_cols[3]:
            st.caption(f"已保存 URL: {st.session_state.ui_saved_url}")

if save_url:
    st.session_state.ui_saved_url = url_draft.strip() or TARGET_URL
    st.session_state.ui_url_draft = st.session_state.ui_saved_url
    _save_ui_settings()
    st.rerun()
elif cancel_url:
    st.session_state.ui_url_draft = st.session_state.ui_saved_url
    st.rerun()
elif restore_default_url:
    st.session_state.ui_saved_url = TARGET_URL
    st.session_state.ui_url_draft = TARGET_URL
    _save_ui_settings()
    st.rerun()

with st.container(border=True):
    st.markdown("#### 抓取设置")
    with st.form("run_form", clear_on_submit=False):
        form_cols = st.columns([1, 1, 1.2, 1])
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
        with form_cols[3]:
            st.write("")
            st.write("")
        action_cols = st.columns([1.2, 1.2, 3.6])
        with action_cols[0]:
            sync_reference = st.form_submit_button("同步 MongoDB 官方说明", use_container_width=True)
        with action_cols[1]:
            submitted = st.form_submit_button("开始抓取并分析", type="primary", use_container_width=True)
        with action_cols[2]:
            reference_meta = st.session_state.reference_metadata or {}
            st.caption("开始抓取并分析会获取 Oracle 最新文档中对 MongoDB API 的支持情况，并结合本地已缓存的 MongoDB 官方说明进行展示。")
            if reference_meta:
                st.caption(
                    "说明缓存: "
                    f"{reference_meta.get('entry_count', 0)} 条, "
                    f"上次同步 {reference_meta.get('synced_at', '未知')}"
                )
            else:
                st.caption("说明缓存: 尚未同步")
            st.caption(
                "说明更新仅在点击“同步 MongoDB 官方说明”时触发；"
                "开始抓取并分析只会使用本地缓存，不会自动更新。"
            )
            st.caption(
                "勾选“显示执行日志（排查问题）”后，同步 MongoDB 官方说明的抓取和缓存写入过程也会输出日志。"
            )

log_container = st.empty()


def _drop_empty_columns(df):
    filtered = df.copy()
    keep_cols = []
    for col in filtered.columns:
        series = filtered[col].fillna("").astype(str).str.strip()
        if series.ne("").any():
            keep_cols.append(col)
    return filtered[keep_cols]


def _reorder_detail_columns(df):
    cols = list(df.columns)
    first_cols = []
    for col in ["Command", "Operator", "Stage", "Support (Since)", "mongo_short_description", "mongo_doc_url"]:
        if col in cols:
            first_cols.append(col)
    remaining = [c for c in cols if c not in first_cols]
    return df[first_cols + remaining]


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


def _prepare_detail_display_df(df: pd.DataFrame, include_section: bool = False) -> pd.DataFrame:
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
    prepared = _drop_empty_columns(prepared)
    prepared = _reorder_detail_columns(prepared)
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
        "oracleDocVersionDate": doc_metadata.get("doc_version_date", "") or "未解析到",
        "oracleDocId": doc_metadata.get("doc_id", "") or "未解析到",
        "oracleDocSourceUrl": doc_metadata.get("doc_source_url", ""),
        "oracleUpdateStatus": doc_metadata.get("update_status", ""),
        "mongodbManualVersion": str(reference_metadata.get("mongodb_manual_version", "")) or "未同步",
        "mongodbManualAboutUrl": str(reference_metadata.get("mongodb_manual_about_url", "")),
        "mongodbSyncedAt": str(reference_metadata.get("synced_at", "未知")),
        "detailColumns": detail_columns,
        "detailRows": detail_rows,
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
    .text-input, .multi-select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel-soft);
      color: var(--text);
      padding: 10px 12px;
      font-size: 14px;
    }}
    .multi-select {{
      min-height: 160px;
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
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
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
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }}
    .section-group summary::-webkit-details-marker {{
      display: none;
    }}
    .section-count {{
      color: var(--muted);
      font-weight: 600;
      font-size: 13px;
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
    a {{
      color: var(--brand-2);
      text-decoration: none;
    }}
    a:hover {{
      text-decoration: underline;
    }}
    @media (max-width: 960px) {{
      .meta-grid, .panel-grid, .metric-grid, .filter-grid {{
        grid-template-columns: 1fr;
      }}
      .bar-row {{
        grid-template-columns: 1fr;
      }}
      .multi-select {{
        min-height: 120px;
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

    <div class="meta-grid">
      <div class="meta-card"><div class="label">结果目录</div><div class="value">{html_lib.escape(output_dir)}</div></div>
      <div class="meta-card"><div class="label">Oracle 文档版本时间</div><div class="value">{html_lib.escape(doc_metadata.get('doc_version_date', '') or '未解析到')}</div></div>
      <div class="meta-card"><div class="label">Oracle 文档编号</div><div class="value">{html_lib.escape(doc_metadata.get('doc_id', '') or '未解析到')}</div></div>
      <div class="meta-card"><div class="label">MongoDB 手册版本</div><div class="value">{html_lib.escape(str(reference_metadata.get('mongodb_manual_version', '')) or '未同步')}</div></div>
    </div>

    <div class="panel">
      <h2>版本来源</h2>
      <div class="version-lines">
        <div class="note">Oracle 文档来源: {html_lib.escape(doc_metadata.get('doc_source_url', ''))}</div>
        <div class="note">MongoDB 版本来源: {html_lib.escape(str(reference_metadata.get('mongodb_manual_about_url', '')))}</div>
        <div class="note">Oracle 版本判断: {html_lib.escape(doc_metadata.get('update_status', ''))}</div>
        <div class="note">MongoDB 说明同步时间: {html_lib.escape(str(reference_metadata.get('synced_at', '未知')))}</div>
      </div>
    </div>

    <div class="panel-grid">
      <section class="panel">
        <h2>API 支持汇总</h2>
        <div id="status-chart" class="chart-block"></div>
        <div id="status-table" class="table-wrap"></div>
      </section>
      <section class="panel">
        <h2>Oracle 启始支持的版本</h2>
        <div id="support-chart" class="chart-block"></div>
        <div id="support-table" class="table-wrap"></div>
      </section>
    </div>

    <section class="panel">
      <h2>MongoDB 命令类型分布</h2>
      <div id="section-chart" class="chart-block"></div>
      <div id="section-table" class="table-wrap"></div>
    </section>

    <section class="panel">
      <h2>Feature Support 明细</h2>
      <div class="filter-grid">
        <div class="filter-control">
          <label for="keyword-filter">关键字搜索</label>
          <input id="keyword-filter" class="text-input" type="text" placeholder="搜索 Command / Operator / Stage / 功能说明" />
        </div>
        <div class="filter-control">
          <label for="section-filter">按 section 筛选</label>
          <select id="section-filter" class="multi-select" multiple></select>
        </div>
        <div class="filter-control">
          <label for="support-filter">按 Support (Since) 筛选</label>
          <select id="support-filter" class="multi-select" multiple></select>
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
      <div class="metric-grid">
        <div class="meta-card"><div class="label">当前命中条数</div><div id="metric-rows" class="value">0</div></div>
        <div class="meta-card"><div class="label">当前 section 数</div><div id="metric-sections" class="value">0</div></div>
        <div class="meta-card"><div class="label">当前说明覆盖</div><div id="metric-description" class="value">0</div></div>
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

    function selectedValues(selectElement) {{
      return Array.from(selectElement.selectedOptions).map((option) => option.value);
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

    function populateSelect(selectElement, values) {{
      selectElement.innerHTML = values.map((value) => (
        `<option value="${{escapeHtml(value)}}">${{escapeHtml(value)}}</option>`
      )).join("");
      const size = Math.min(Math.max(values.length, 4), 10);
      selectElement.size = size;
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

      renderMetrics(filteredRows);
      renderSummaries(filteredRows);
      renderDetails(filteredRows);
    }}

    function renderMetrics(rows) {{
      const sectionCount = new Set(rows.map((row) => row.__section || "Unknown Section")).size;
      const descriptionCount = rows.filter((row) => String(row["功能说明"] || "").trim()).length;
      document.getElementById("metric-rows").textContent = String(rows.length);
      document.getElementById("metric-sections").textContent = String(sectionCount);
      document.getElementById("metric-description").textContent = String(descriptionCount);
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
        "Oracle 启始支持的版本",
        supportRows,
        (_, index) => palette[index % palette.length]
      );
      renderBars(
        "section-chart",
        "MongoDB 命令类型分布",
        sectionRows,
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
      document.getElementById("section-table").innerHTML = createTable(
        ["section", "count", "percentage"],
        sectionRows.map((item) => ({{
          section: item.label,
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
        const tableRows = sectionRows.map((row) => {{
          const record = {{}};
          reportData.detailColumns.forEach((column) => {{
            record[column] = row[column] || "";
          }});
          return record;
        }});
        return `
          <details class="section-group">
            <summary>
              <span>${{escapeHtml(section)}}</span>
              <span class="section-count">${{sectionRows.length}} 条</span>
            </summary>
            <div class="section-body table-wrap">
              ${{createTable(reportData.detailColumns, tableRows, ["MongoDB 官方文档"])}}
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
      Array.from(sectionFilter.options).forEach((option) => {{
        option.selected = false;
      }});
      Array.from(supportFilter.options).forEach((option) => {{
        option.selected = false;
      }});
      descOnlyFilter.checked = false;
      applyFilters();
    }}

    function init() {{
      populateSelect(sectionFilter, uniqueSorted(reportData.detailRows.map((row) => row.__section || "Unknown Section")));
      populateSelect(supportFilter, uniqueSorted(reportData.detailRows.map((row) => row.__support_since || "Unknown")));

      keywordFilter.addEventListener("input", applyFilters);
      sectionFilter.addEventListener("change", applyFilters);
      supportFilter.addEventListener("change", applyFilters);
      descOnlyFilter.addEventListener("change", applyFilters);
      clearFiltersButton.addEventListener("click", clearFilters);
      expandAllButton.addEventListener("click", () => expandOrCollapseAll(true));
      collapseAllButton.addEventListener("click", () => expandOrCollapseAll(false));

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

    with st.spinner("正在抓取并分析，请稍候..."):
        try:
            result = analyze_feature_support(
                url=st.session_state.ui_saved_url,
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

    st.markdown("### 总览")
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
        st.success(f"分析完成，结果已保存到: {output_dir}")

    if doc_metadata:
        st.caption(f"版本信息来源: {INDEX_URL}")
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
        if reference_metadata.get("mongodb_manual_about_url"):
            st.caption(
                "MongoDB 版本来源: "
                f"{reference_metadata.get('mongodb_manual_about_url')}"
            )

    st.markdown("### 统计分析")
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
            st.subheader("Oracle 启始支持的版本")
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

    if not section_display_df.empty:
        with st.container(border=True):
            st.subheader("MongoDB 命令类型分布")
            section_chart_source = section_display_df.sort_values("count", ascending=False).copy()
            section_chart_height = min(max(360, len(section_chart_source) * 22), 760)
            section_chart = (
                alt.Chart(section_chart_source)
                .mark_bar(cornerRadiusEnd=3)
                .encode(
                    y=alt.Y("section:N", sort="-x", title="命令类型"),
                    x=alt.X("count:Q", title="数量"),
                    color=alt.Color(
                        "count:Q",
                        scale=alt.Scale(scheme="tealblues"),
                        legend=None,
                    ),
                    tooltip=["section", "count", "percentage"],
                )
                .properties(height=section_chart_height)
            )
            st.altair_chart(section_chart, use_container_width=True)
            st.dataframe(section_display_df, use_container_width=True, height=280)

    with st.container(border=True):
        st.subheader("Feature Support 明细")

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

        filter_stats = st.columns(3)
        with filter_stats[0]:
            st.metric("当前命中条数", len(filtered_detail_df))
        with filter_stats[1]:
            matched_sections = (
                filtered_detail_df["section"].dropna().astype(str).nunique()
                if "section" in filtered_detail_df.columns
                else 0
            )
            st.metric("当前 section 数", matched_sections)
        with filter_stats[2]:
            matched_descriptions = (
                filtered_detail_df["mongo_short_description"].fillna("").astype(str).str.strip().ne("").sum()
                if "mongo_short_description" in filtered_detail_df.columns
                else 0
            )
            st.metric("当前说明覆盖", int(matched_descriptions))

        if "section" in filtered_detail_df.columns:
            grouped = filtered_detail_df.groupby("section", dropna=False, sort=False)
            for section_name, sec_df in grouped:
                section_text = str(section_name) if str(section_name).strip() else "Unknown Section"
                with st.expander(f"{section_text} ({len(sec_df)} 条)", expanded=False):
                    sec_display_df = sec_df.drop(columns=["section"], errors="ignore")
                    sec_display_df = sec_display_df.drop(columns=["table_index"], errors="ignore")
                    sec_display_df = sec_display_df.drop(columns=["mongo_entity_type"], errors="ignore")
                    sec_display_df = sec_display_df.drop(columns=["mongo_source_group"], errors="ignore")
                    sec_display_df = sec_display_df.drop(columns=["mongo_name"], errors="ignore")
                    sec_display_df = sec_display_df.drop(columns=["mongo_reference_category"], errors="ignore")
                    sec_display_df = sec_display_df.drop(columns=["mongo_last_synced_at"], errors="ignore")
                    sec_display_df = _drop_empty_columns(sec_display_df)
                    sec_display_df = _reorder_detail_columns(sec_display_df)
                    sec_display_df = sec_display_df.rename(
                        columns={
                            "mongo_short_description": "功能说明",
                            "mongo_doc_url": "MongoDB 官方文档",
                        }
                    )
                    st.dataframe(sec_display_df, use_container_width=True, height=320)
        else:
            detail_display_df = filtered_detail_df.drop(columns=["table_index"], errors="ignore")
            detail_display_df = detail_display_df.drop(columns=["mongo_entity_type"], errors="ignore")
            detail_display_df = detail_display_df.drop(columns=["mongo_source_group"], errors="ignore")
            detail_display_df = detail_display_df.drop(columns=["mongo_name"], errors="ignore")
            detail_display_df = detail_display_df.drop(columns=["mongo_reference_category"], errors="ignore")
            detail_display_df = detail_display_df.drop(columns=["mongo_last_synced_at"], errors="ignore")
            detail_display_df = _drop_empty_columns(detail_display_df)
            detail_display_df = _reorder_detail_columns(detail_display_df)
            detail_display_df = detail_display_df.rename(
                columns={
                    "mongo_short_description": "功能说明",
                    "mongo_doc_url": "MongoDB 官方文档",
                }
            )
            st.dataframe(detail_display_df, use_container_width=True, height=500)

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
