from __future__ import annotations

import altair as alt
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

if not st.session_state.settings_loaded:
    saved_settings = {}
    if SETTINGS_PATH.exists():
        try:
            saved_settings = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            saved_settings = {}
    st.session_state.ui_url = saved_settings.get("url", TARGET_URL)
    st.session_state.ui_timeout = int(saved_settings.get("timeout", 60))
    st.session_state.ui_max_retries = int(saved_settings.get("max_retries", 3))
    st.session_state.ui_show_debug = bool(saved_settings.get("show_debug_log", True))
    st.session_state.settings_loaded = True

if st.session_state.reference_df.empty:
    st.session_state.reference_df = load_mongodb_reference_catalog()
if not st.session_state.reference_metadata:
    st.session_state.reference_metadata = load_mongodb_reference_metadata()

with st.container(border=True):
    st.markdown("#### 抓取设置")
    with st.form("run_form", clear_on_submit=False):
        url = st.text_input("目标链接", key="ui_url")
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
            submitted = st.form_submit_button("开始抓取并分析", type="primary")

sync_cols = st.columns([1.25, 4])
with sync_cols[0]:
    sync_reference = st.button("同步 MongoDB 官方说明", use_container_width=True)
with sync_cols[1]:
    reference_meta = st.session_state.reference_metadata or {}
    if reference_meta:
        st.caption(
            "MongoDB 说明缓存: "
            f"{reference_meta.get('entry_count', 0)} 条, "
            f"上次同步 {reference_meta.get('synced_at', '未知')}"
        )
    else:
        st.caption("MongoDB 说明缓存: 尚未同步")

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
        log_container.code("\n".join(st.session_state.debug_logs[-200:]), language="text")

if sync_reference:
    st.session_state.debug_logs = []
    st.session_state.show_debug_log_runtime = show_debug_log
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
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(
        json.dumps(
            {
                "url": url,
                "timeout": int(timeout),
                "max_retries": int(max_retries),
                "show_debug_log": bool(show_debug_log),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
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
                url=url,
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
    log_container.code("\n".join(st.session_state.debug_logs[-200:]), language="text")

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
    overview_cols = st.columns([1, 1, 1.2, 1.2])
    with overview_cols[0]:
        st.metric("明细记录数", len(enriched_detail_df))
    with overview_cols[1]:
        st.metric("文档版本时间", doc_metadata.get("doc_version_date", "") or "未解析到")
    with overview_cols[2]:
        st.metric("文档编号", doc_metadata.get("doc_id", "") or "未解析到")
    with overview_cols[3]:
        st.metric("说明已覆盖", description_match_count)

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
            f"| 新增 {reference_metadata.get('new_entry_count', 0)} "
            f"| 更新 {reference_metadata.get('updated_entry_count', 0)}"
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
        if "section" in enriched_detail_df.columns:
            grouped = enriched_detail_df.groupby("section", dropna=False, sort=False)
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
            detail_display_df = enriched_detail_df.drop(columns=["table_index"], errors="ignore")
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

    download_cols = st.columns([1, 1, 4])
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
