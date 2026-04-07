from __future__ import annotations

import altair as alt
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from src.oracle_feature_support.fetcher import INDEX_URL, TARGET_URL, analyze_feature_support

SETTINGS_PATH = Path("outputs/.ui_settings.json")

st.set_page_config(page_title="Oracle MongoDB API Feature Support", layout="wide")
st.title("Oracle MongoDB API - Feature Support")
st.caption("手工点击按钮后才会执行抓取与统计")

if "result_detail_df" not in st.session_state:
    st.session_state.result_detail_df = None
if "result_summary_df" not in st.session_state:
    st.session_state.result_summary_df = None
if "result_output_dir" not in st.session_state:
    st.session_state.result_output_dir = None
if "doc_metadata" not in st.session_state:
    st.session_state.doc_metadata = {}
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

with st.form("run_form", clear_on_submit=False):
    url = st.text_input("目标链接", key="ui_url")
    timeout = st.number_input("请求超时（秒）", min_value=10, max_value=180, step=5, key="ui_timeout")
    max_retries = st.number_input("重试次数", min_value=0, max_value=6, step=1, key="ui_max_retries")
    show_debug_log = st.checkbox("显示执行日志（排查问题）", key="ui_show_debug")
    submitted = st.form_submit_button("开始抓取并分析", type="primary")

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
    for col in ["Operator", "Support (Since)"]:
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
    has_metric = "metric" in summary_df.columns

    if st.session_state.restored_from_disk:
        last_refresh_time = datetime.fromtimestamp(Path(output_dir).stat().st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        st.info(f"上次刷新时间: {last_refresh_time}")
    else:
        st.success(f"分析完成，结果已保存到: {output_dir}")

    if doc_metadata:
        doc_cols = st.columns(2)
        with doc_cols[0]:
            st.metric("文档版本时间", doc_metadata.get("doc_version_date", "") or "未解析到")
        with doc_cols[1]:
            st.metric("文档编号", doc_metadata.get("doc_id", "") or "未解析到")
        st.caption(f"版本信息来源: {INDEX_URL}")

        update_status = doc_metadata.get("update_status", "")
        if "发现文档版本变化" in update_status:
            st.warning(update_status)
        elif update_status:
            st.info(update_status)

    status_summary_df_raw = (
        summary_df[summary_df["metric"] == "normalized_status"].copy()
        if has_metric
        else summary_df.copy()
    )
    status_summary_df = status_summary_df_raw[["normalized_status", "count", "percentage"]]

    st.metric("明细记录数", len(detail_df))

    st.subheader("API 支持汇总")
    st.dataframe(status_summary_df, use_container_width=True, height=220)
    status_color_domain = ["Supported", "Not Supported", "Partially Supported", "Other"]
    status_color_range = ["#2E8B57", "#D9534F", "#F0AD4E", "#7A7A7A"]
    status_chart = (
        alt.Chart(status_summary_df)
        .mark_bar()
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

    if has_metric:
        support_top_df = summary_df[summary_df["metric"] == "support_value_top20"].copy()
        if not support_top_df.empty:
            st.subheader("Oracle 启始支持的版本")
            support_top_display_df = support_top_df.rename(
                columns={"normalized_status": "support_since"}
            )
            support_top_display_df = support_top_display_df[
                ["support_since", "count", "percentage"]
            ]
            st.dataframe(support_top_display_df, use_container_width=True, height=280)
            pie_data = support_top_display_df.copy()
            pie_data["support_since"] = pie_data["support_since"].astype(str)
            pie_chart = (
                alt.Chart(pie_data)
                .mark_arc()
                .encode(
                    theta=alt.Theta(field="count", type="quantitative"),
                    color=alt.Color(field="support_since", type="nominal", title="Support Since"),
                    tooltip=["support_since", "count", "percentage"],
                )
            )
            st.altair_chart(pie_chart, use_container_width=True)

        section_df = summary_df[summary_df["metric"] == "section_count"].copy()
        if not section_df.empty:
            st.subheader("MongoDB 命令类型分布")
            section_display_df = section_df.rename(columns={"normalized_status": "section"})
            section_display_df = section_display_df[["section", "count", "percentage"]]
            st.dataframe(section_display_df, use_container_width=True, height=280)
            section_chart_source = section_display_df.sort_values("count", ascending=False).copy()
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
            )
            st.altair_chart(section_chart, use_container_width=True)

    st.subheader("Feature Support 明细")
    if "section" in detail_df.columns:
        grouped = detail_df.groupby("section", dropna=False, sort=False)
        for section_name, sec_df in grouped:
            section_text = str(section_name) if str(section_name).strip() else "Unknown Section"
            with st.expander(f"{section_text} ({len(sec_df)} 条)", expanded=False):
                sec_display_df = sec_df.drop(columns=["section"], errors="ignore")
                sec_display_df = sec_display_df.drop(columns=["table_index"], errors="ignore")
                sec_display_df = _drop_empty_columns(sec_display_df)
                sec_display_df = _reorder_detail_columns(sec_display_df)
                st.dataframe(sec_display_df, use_container_width=True, height=320)
    else:
        detail_display_df = detail_df.drop(columns=["table_index"], errors="ignore")
        detail_display_df = _drop_empty_columns(detail_display_df)
        detail_display_df = _reorder_detail_columns(detail_display_df)
        st.dataframe(detail_display_df, use_container_width=True, height=500)

    st.download_button(
        "下载明细 CSV",
        data=detail_df.to_csv(index=False).encode("utf-8-sig"),
        file_name="feature_support_detail.csv",
        mime="text/csv",
    )
    st.download_button(
        "下载统计 CSV",
        data=summary_df.to_csv(index=False).encode("utf-8-sig"),
        file_name="feature_support_summary.csv",
        mime="text/csv",
    )
