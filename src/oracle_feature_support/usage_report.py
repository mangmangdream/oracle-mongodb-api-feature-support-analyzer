from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path

import pandas as pd


@dataclass
class UsageAnalysisArtifacts:
    detail_df: pd.DataFrame
    summary_df: pd.DataFrame
    output_dir: Path
    detail_path: Path
    summary_path: Path
    metadata_path: Path


def build_usage_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(
            columns=["metric", "label", "count", "usage_count", "percentage"]
        )

    total_rows = max(len(detail_df), 1)
    total_usage = max(int(detail_df["usage_count"].fillna(0).sum()), 1)
    frames: list[pd.DataFrame] = []

    status = (
        detail_df.groupby("oracle_support_status", dropna=False)
        .agg(count=("feature_name", "size"), usage_count=("usage_count", "sum"))
        .reset_index()
        .rename(columns={"oracle_support_status": "label"})
    )
    status["metric"] = "oracle_support_status"
    status["percentage"] = (status["count"] / total_rows * 100).round(2)
    frames.append(status)

    feature_type = (
        detail_df.groupby("feature_type", dropna=False)
        .agg(count=("feature_name", "size"), usage_count=("usage_count", "sum"))
        .reset_index()
        .rename(columns={"feature_type": "label"})
    )
    feature_type["metric"] = "feature_type"
    feature_type["percentage"] = (feature_type["usage_count"] / total_usage * 100).round(2)
    frames.append(feature_type)

    command = (
        detail_df.groupby("command_name", dropna=False)
        .agg(count=("feature_name", "size"), usage_count=("usage_count", "sum"))
        .reset_index()
        .rename(columns={"command_name": "label"})
    )
    command["metric"] = "command_name"
    command["percentage"] = (command["usage_count"] / total_usage * 100).round(2)
    frames.append(command)

    return pd.concat(frames, ignore_index=True)[
        ["metric", "label", "count", "usage_count", "percentage"]
    ]


def write_usage_analysis_outputs(
    detail_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    metadata: dict[str, object],
    output_root: str = "outputs",
) -> UsageAnalysisArtifacts:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(output_root) / f"mongodb_usage_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    detail_path = output_dir / "mongodb_usage_feature_detail.csv"
    summary_path = output_dir / "mongodb_usage_feature_summary.csv"
    metadata_path = output_dir / "mongodb_usage_metadata.json"

    detail_df.to_csv(detail_path, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    return UsageAnalysisArtifacts(
        detail_df=detail_df,
        summary_df=summary_df,
        output_dir=output_dir,
        detail_path=detail_path,
        summary_path=summary_path,
        metadata_path=metadata_path,
    )
