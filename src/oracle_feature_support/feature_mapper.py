from __future__ import annotations

import pandas as pd

from .fetcher import _normalize_status


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_key(value: object) -> str:
    return _clean_text(value).lower()


def map_features_to_oracle_support(
    usage_df: pd.DataFrame,
    oracle_detail_df: pd.DataFrame,
) -> pd.DataFrame:
    if usage_df.empty:
        empty = usage_df.copy()
        empty["oracle_support_status"] = ""
        empty["oracle_support_since"] = ""
        empty["oracle_category"] = ""
        empty["oracle_feature"] = ""
        return empty

    mapped = usage_df.copy()
    mapped["oracle_support_status"] = "Unknown"
    mapped["oracle_support_since"] = ""
    mapped["oracle_category"] = ""
    mapped["oracle_feature"] = ""

    if oracle_detail_df.empty:
        return mapped

    oracle_df = oracle_detail_df.copy()
    for column in ["Command", "Operator", "Stage", "Support (Since)", "section", "normalized_status"]:
        if column not in oracle_df.columns:
            oracle_df[column] = ""

    command_map = (
        oracle_df.loc[oracle_df["Command"].fillna("").astype(str).str.strip().ne("")]
        .assign(_match_key=lambda df: df["Command"].map(_normalize_key))
        .drop_duplicates("_match_key", keep="first")
        .set_index("_match_key")
    )
    stage_map = (
        oracle_df.loc[oracle_df["Stage"].fillna("").astype(str).str.strip().ne("")]
        .assign(_match_key=lambda df: df["Stage"].map(_normalize_key))
        .drop_duplicates("_match_key", keep="first")
        .set_index("_match_key")
    )
    operator_map = (
        oracle_df.loc[oracle_df["Operator"].fillna("").astype(str).str.strip().ne("")]
        .assign(_match_key=lambda df: df["Operator"].map(_normalize_key))
        .drop_duplicates("_match_key", keep="first")
        .set_index("_match_key")
    )

    def resolve_row(feature_type: str, feature_name: str) -> pd.Series | None:
        key = _normalize_key(feature_name)
        if feature_type == "command" and key in command_map.index:
            return command_map.loc[key]
        if feature_type == "stage" and key in stage_map.index:
            return stage_map.loc[key]
        if feature_type in {"operator", "expression"} and key in operator_map.index:
            return operator_map.loc[key]
        return None

    for index, row in mapped.iterrows():
        matched = resolve_row(str(row["feature_type"]), str(row["feature_name"]))
        if matched is None:
            continue
        support_since = _clean_text(matched.get("Support (Since)", ""))
        normalized_status = (
            _normalize_status(support_since)
            if support_since
            else (_clean_text(matched.get("normalized_status", "")) or "Unknown")
        )
        mapped.at[index, "oracle_support_status"] = normalized_status or "Unknown"
        mapped.at[index, "oracle_support_since"] = support_since
        mapped.at[index, "oracle_category"] = _clean_text(matched.get("section", ""))
        mapped.at[index, "oracle_feature"] = _clean_text(
            matched.get("Command", "")
            or matched.get("Stage", "")
            or matched.get("Operator", "")
        )

    return mapped
