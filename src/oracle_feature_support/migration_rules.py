from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


DEFAULT_RULES_DIR = Path(__file__).resolve().parents[2] / "config" / "migration_rules"
RULE_COLUMNS = [
    "feature_name",
    "feature_type",
    "migration_scope",
    "scope_confidence",
    "needs_review",
    "default_complexity",
    "complexity_reason",
    "recommended_action",
    "review_priority",
    "notes",
    "enabled",
]
OVERRIDE_COLUMNS = [
    "feature_type",
    "feature_name",
    "override_scope",
    "override_complexity",
    "override_reason",
    "override_action",
    "enabled",
]
ALLOWED_SCOPES = {"application_api", "likely_admin", "ignore"}
ALLOWED_SCOPE_CONFIDENCE = {"high", "medium", "low"}
ALLOWED_COMPLEXITY = {"Ignore", "Low", "Medium", "High", "Blocker"}
ALLOWED_ACTIONS = {
    "direct_use",
    "verify_semantics",
    "minor_query_rewrite",
    "minor_update_rewrite",
    "rewrite_aggregation",
    "replace_operator",
    "review_index_management",
    "exclude_from_scope",
    "manual_assessment",
    "migration_blocker_review",
}
ALLOWED_REVIEW_PRIORITY = {"P0", "P1", "P2"}


@dataclass(frozen=True)
class MigrationRuleSet:
    manifest: dict[str, object]
    baseline_df: pd.DataFrame
    override_df: pd.DataFrame

    @property
    def rules_version(self) -> str:
        return str(self.manifest.get("rules_version", "") or "")


def _read_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df[columns].copy()


def _normalize_bool_series(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .isin({"1", "true", "yes", "y"})
    )


def _normalize_rule_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in RULE_COLUMNS:
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
    normalized["enabled"] = _normalize_bool_series(normalized["enabled"])
    normalized["needs_review"] = _normalize_bool_series(normalized["needs_review"])
    normalized = normalized[normalized["enabled"]].copy()
    return normalized.reset_index(drop=True)


def _normalize_override_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in OVERRIDE_COLUMNS:
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
    normalized["enabled"] = _normalize_bool_series(normalized["enabled"])
    normalized = normalized[normalized["enabled"]].copy()
    return normalized.reset_index(drop=True)


def _require_values(frame: pd.DataFrame, column: str, allowed: set[str], label: str) -> None:
    values = {
        str(value).strip()
        for value in frame[column].fillna("").astype(str).tolist()
        if str(value).strip()
    }
    invalid = sorted(values - allowed)
    if invalid:
        raise ValueError(f"{label} contains invalid values: {', '.join(invalid)}")


def validate_rule_frames(baseline_df: pd.DataFrame, override_df: pd.DataFrame) -> None:
    if baseline_df.empty:
        return

    _require_values(baseline_df, "migration_scope", ALLOWED_SCOPES, "migration rules")
    _require_values(
        baseline_df,
        "scope_confidence",
        ALLOWED_SCOPE_CONFIDENCE,
        "migration rules",
    )
    _require_values(
        baseline_df,
        "default_complexity",
        ALLOWED_COMPLEXITY,
        "migration rules",
    )
    _require_values(
        baseline_df,
        "recommended_action",
        ALLOWED_ACTIONS,
        "migration rules",
    )
    _require_values(
        baseline_df,
        "review_priority",
        ALLOWED_REVIEW_PRIORITY,
        "migration rules",
    )

    if not override_df.empty:
        if "override_scope" in override_df.columns:
            _require_values(
                override_df.loc[override_df["override_scope"].str.strip().ne("")],
                "override_scope",
                ALLOWED_SCOPES,
                "customer overrides",
            )
        if "override_complexity" in override_df.columns:
            _require_values(
                override_df.loc[override_df["override_complexity"].str.strip().ne("")],
                "override_complexity",
                ALLOWED_COMPLEXITY,
                "customer overrides",
            )
        if "override_action" in override_df.columns:
            _require_values(
                override_df.loc[override_df["override_action"].str.strip().ne("")],
                "override_action",
                ALLOWED_ACTIONS,
                "customer overrides",
            )


def load_customer_overrides(rules_dir: Path = DEFAULT_RULES_DIR) -> pd.DataFrame:
    override_path = rules_dir / "customer_overrides.csv"
    return _normalize_override_frame(_read_csv(override_path, OVERRIDE_COLUMNS))


def save_customer_overrides(
    override_df: pd.DataFrame,
    rules_dir: Path = DEFAULT_RULES_DIR,
) -> Path:
    writable = override_df.copy()
    for column in OVERRIDE_COLUMNS:
        if column not in writable.columns:
            writable[column] = ""
    writable = writable[OVERRIDE_COLUMNS].fillna("")
    writable["enabled"] = _normalize_bool_series(writable["enabled"]).map(
        lambda value: "true" if value else "false"
    )
    normalized_override_df = _normalize_override_frame(writable)
    validate_rule_frames(pd.DataFrame(columns=RULE_COLUMNS), normalized_override_df)
    target_path = rules_dir / "customer_overrides.csv"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    writable.sort_values(
        by=["feature_type", "feature_name"],
        ascending=[True, True],
    ).to_csv(target_path, index=False, encoding="utf-8-sig")
    return target_path


def load_migration_rules(rules_dir: Path = DEFAULT_RULES_DIR) -> MigrationRuleSet:
    manifest_path = rules_dir / "rules_manifest.json"
    manifest = (
        json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest_path.exists()
        else {}
    )
    files = manifest.get("files", []) if isinstance(manifest, dict) else []
    frames: list[pd.DataFrame] = []
    for file_name in files:
        frame = _read_csv(rules_dir / str(file_name), RULE_COLUMNS)
        if not frame.empty:
            frames.append(frame)
    baseline_df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=RULE_COLUMNS)
    )
    baseline_df = _normalize_rule_frame(baseline_df)
    override_df = load_customer_overrides(rules_dir)
    validate_rule_frames(baseline_df, override_df)
    return MigrationRuleSet(
        manifest=manifest if isinstance(manifest, dict) else {},
        baseline_df=baseline_df,
        override_df=override_df,
    )
