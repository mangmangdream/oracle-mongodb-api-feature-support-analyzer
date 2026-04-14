from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .migration_rules import MigrationRuleSet


COMPLEXITY_ORDER = {
    "Ignore": 0,
    "Low": 1,
    "Medium": 2,
    "High": 3,
    "Blocker": 4,
}
PRIORITY_ORDER = {
    "Low": 0,
    "Medium": 1,
    "High": 2,
    "Critical": 3,
}
MIGRATION_NECESSITY_BY_SCOPE = {
    "application_api": "Required",
    "likely_admin": "Conditional",
    "ignore": "Unnecessary",
}
BLOCKER_CANDIDATE_FEATURES = {
    ("stage", "$graphLookup"),
}
ADMIN_COMMAND_NAMES = {
    "abortmovecollection",
    "abortreshardcollection",
    "killop",
    "currentop",
    "listdatabases",
    "serverstatus",
    "dbstats",
    "collstats",
    "usersinfo",
    "replsetgetstatus",
    "connectionstatus",
    "buildinfo",
    "getparameter",
    "hostinfo",
    "ping",
    "top",
    "whatsmyuri",
    "getlog",
    "connpoolstats",
    "listcommands",
    "hello",
    "isdbgrid",
    "logout",
    "authenticate",
    "profile",
    "validate",
    "validatedbmetadata",
    "datasize",
    "addshard",
    "addshardtozone",
    "analyzeshardkey",
    "appendoplognote",
    "applyops",
    "balancercollectionstatus",
    "balancerstart",
    "balancerstatus",
    "balancerstop",
    "checkmetadataconsistency",
    "cleanupreshardcollection",
    "clearjumboflag",
    "collmod",
    "compact",
    "compactstructuredencryptiondata",
    "configurecollectionbalancing",
    "configurequeryanalyzer",
    "createrole",
    "createsearchindexes",
    "createuser",
    "dbhash",
    "dropallrolesfromdatabase",
    "dropallusersfromdatabase",
    "dropconnections",
    "dropdatabase",
    "droprole",
    "dropsearchindex",
    "dropuser",
    "enablesharding",
    "explain",
    "flushrouterconfig",
    "getauditconfig",
    "getclusterparameter",
    "getcmdlineopts",
    "getdefaultrwconcern",
    "getshardmap",
    "grantprivilegestorole",
    "grantrolestorole",
    "grantrolestouser",
    "invalidateusercache",
    "killallsessions",
    "killallsessionsbypattern",
    "killcursors",
    "killsessions",
    "listcollections",
    "listindexes",
    "listshards",
    "lockinfo",
    "logapplicationmessage",
    "logrotate",
    "mergeallchunksonshard",
    "mergechunks",
    "movechunk",
    "movecollection",
    "moveprimary",
    "moverange",
    "plancacheclear",
    "plancacheclearfilters",
    "plancachelistfilters",
    "plancachesetfilter",
    "reindex",
    "refinecollectionshardkey",
    "refreshsessions",
    "removeshard",
    "removeshardfromzone",
    "replsetabortprimarycatchup",
    "replsetfreeze",
    "replsetgetconfig",
    "replsetinitiate",
    "replsetmaintenance",
    "replsetreconfig",
    "replsetresizeoplog",
    "replsetstepdown",
    "replsetsyncfrom",
    "reshardcollection",
    "revokeprivilegesfromrole",
    "revokerolesfromrole",
    "revokerolesfromuser",
    "rolesinfo",
    "rotatecertificates",
    "setallowmigrations",
    "setauditconfig",
    "setclusterparameter",
    "setdefaultrwconcern",
    "setfeaturecompatibilityversion",
    "setindexcommitquorum",
    "setparameter",
    "setuserwriteblockmode",
    "shardcollection",
    "shardconnpoolstats",
    "shardingstate",
    "split",
    "transitionfromdedicatedconfigserver",
    "transitiontodedicatedconfigserver",
    "unsetsharding",
    "unshardcollection",
    "updaterole",
    "updatesearchindex",
    "updateuser",
    "updatezonekeyrange",
}
ADMIN_COMMAND_PREFIXES = (
    "createuser",
    "dropuser",
    "updateuser",
    "grant",
    "revoke",
    "shutdown",
    "setparameter",
    "fsync",
)
APPLICATION_LOW_COMMANDS = {
    "find",
    "insert",
    "update",
    "delete",
}
APPLICATION_MEDIUM_COMMANDS = {
    "findandmodify",
    "distinct",
    "count",
    "bulkwrite",
    "createindexes",
    "create",
    "committransaction",
    "aborttransaction",
    "startsession",
    "endsessions",
}
APPLICATION_HIGH_COMMANDS = {
    "aggregate",
    "mapreduce",
}
APPLICATION_BLOCKER_CANDIDATE_COMMANDS = {
    "mapreduce",
}
ADMIN_KEYWORDS = (
    "shard",
    "reshard",
    "balancer",
    "zone",
    "replset",
    "audit",
    "parameter",
    "clusterparameter",
    "defaultrwconcern",
    "featurecompatibility",
    "writeblock",
    "router",
    "chunk",
    "move",
    "configserver",
    "log",
    "lock",
    "certificate",
)
CONDITIONAL_OBJECT_MANAGEMENT_COMMANDS = {
    "dropindexes",
    "listindexes",
    "listcollections",
    "renamecollection",
}
NON_CORE_COMPATIBILITY_COMMANDS = {
    "clonecollectionascapped",
    "converttocapped",
    "filemd5",
}


@dataclass
class MigrationAssessmentResult:
    detail_df: pd.DataFrame
    summary_df: pd.DataFrame
    baseline_df: pd.DataFrame
    hotspots_df: pd.DataFrame
    excluded_df: pd.DataFrame
    rules_version: str
    override_count: int
    rules_coverage_rate: float
    unclassified_feature_count: int


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_key(feature_type: object, feature_name: object) -> tuple[str, str]:
    return (_clean_text(feature_type).lower(), _clean_text(feature_name).lower())


def _complexity_rank(value: str) -> int:
    return COMPLEXITY_ORDER.get(_clean_text(value), 0)


def _max_complexity(left: str, right: str) -> str:
    return left if _complexity_rank(left) >= _complexity_rank(right) else right


def _min_complexity(left: str, right: str) -> str:
    return left if _complexity_rank(left) <= _complexity_rank(right) else right


def _migration_necessity(scope: object) -> str:
    normalized_scope = _clean_text(scope).lower()
    return MIGRATION_NECESSITY_BY_SCOPE.get(normalized_scope, "Required")


def _migration_necessity_reason(scope: object, feature_name: object) -> str:
    normalized_scope = _clean_text(scope).lower()
    normalized_feature = _clean_text(feature_name) or "this feature"
    if normalized_scope == "application_api":
        return (
            f"`{normalized_feature}` affects application-visible behavior or application-managed objects, "
            "so it should be considered part of the migration compatibility scope."
        )
    if normalized_scope == "likely_admin":
        return (
            f"`{normalized_feature}` is usually operational or tooling-related. "
            "Keep it only if the customer explicitly needs the same workflow after migration."
        )
    return (
        f"`{normalized_feature}` is mainly administrative or diagnostic and is normally outside the application migration scope."
    )


def _support_floor(row: pd.Series) -> tuple[str, str]:
    status = _clean_text(row.get("oracle_support_status", "")) or "Unknown"
    feature_key = _normalize_key(row.get("feature_type", ""), row.get("feature_name", ""))
    if status == "Not Supported":
        if feature_key in BLOCKER_CANDIDATE_FEATURES:
            return "Blocker", "Oracle support status is Not Supported for a blocker-candidate API."
        return "High", "Oracle support status is Not Supported."
    if status == "Partially Supported":
        return "Medium", "Oracle support status is Partially Supported."
    if status == "Unknown":
        return "Medium", "Oracle support status is Unknown, requiring manual validation."
    return "", ""


def _apply_support_based_complexity(
    current_complexity: str,
    effective_scope: str,
    usage_row: pd.Series,
) -> tuple[str, str]:
    status = _clean_text(usage_row.get("oracle_support_status", "")) or "Unknown"
    if effective_scope != "application_api":
        if effective_scope in {"ignore", "likely_admin"}:
            return "Ignore", "Administrative or operational item is excluded from application migration scope."
        return current_complexity, ""

    if status == "Supported":
        return (
            _min_complexity(current_complexity, "Low"),
            "Oracle support status is Supported, so application code change is expected to be minimal.",
        )

    floor_complexity, floor_reason = _support_floor(usage_row)
    if floor_complexity:
        return _max_complexity(current_complexity, floor_complexity), floor_reason
    return current_complexity, ""


def _looks_like_admin_command(feature_type: str, feature_name: str) -> bool:
    if feature_type != "command":
        return False
    normalized = _clean_text(feature_name).lower()
    if normalized in ADMIN_COMMAND_NAMES:
        return True
    if any(keyword in normalized for keyword in ADMIN_KEYWORDS):
        return True
    return normalized.startswith(ADMIN_COMMAND_PREFIXES)


def _infer_command_baseline(feature_name: str) -> tuple[str, str, str, str, str]:
    normalized = _clean_text(feature_name).lower()
    if normalized in NON_CORE_COMPATIBILITY_COMMANDS:
        return (
            "ignore",
            "Ignore",
            "Specialized MongoDB maintenance command. It is usually not required to preserve application compatibility when migrating to Oracle Database API for MongoDB.",
            "exclude_from_scope",
            "heuristic_command",
        )
    if normalized in CONDITIONAL_OBJECT_MANAGEMENT_COMMANDS:
        return (
            "likely_admin",
            "Ignore",
            "Object-management or inspection command that is only needed if the customer wants to preserve the same operational workflow.",
            "exclude_from_scope",
            "heuristic_command",
        )
    if normalized in APPLICATION_LOW_COMMANDS:
        return (
            "application_api",
            "Low",
            "Core application CRUD command. If Oracle supports it, migration is usually straightforward; if not, complexity rises significantly because business logic depends on it.",
            "direct_use",
            "heuristic_command",
        )
    if normalized in APPLICATION_MEDIUM_COMMANDS:
        return (
            "application_api",
            "Medium",
            "Application-facing command or application-managed object command with option or semantic differences that usually require verification during migration.",
            "verify_semantics",
            "heuristic_command",
        )
    if normalized in APPLICATION_HIGH_COMMANDS:
        action = "migration_blocker_review" if normalized in APPLICATION_BLOCKER_CANDIDATE_COMMANDS else "rewrite_aggregation"
        complexity = "Blocker" if normalized in APPLICATION_BLOCKER_CANDIDATE_COMMANDS else "High"
        return (
            "application_api",
            complexity,
            "Application-facing command with complex processing semantics that usually requires significant rewrite or dedicated migration design.",
            action,
            "heuristic_command",
        )
    if _looks_like_admin_command("command", normalized):
        return (
            "ignore",
            "Ignore",
            "Administrative or operational MongoDB command. It is usually not necessary to preserve this behavior when migrating application workloads to Oracle Database API for MongoDB.",
            "exclude_from_scope",
            "heuristic_command",
        )
    if normalized.startswith("list"):
        return (
            "likely_admin",
            "Ignore",
            "Discovery or inspection command. It is usually not part of core application behavior during migration.",
            "exclude_from_scope",
            "heuristic_command",
        )
    if normalized.startswith(("create", "drop", "grant", "revoke", "updateuser", "updaterole")):
        return (
            "likely_admin",
            "Ignore",
            "Administrative DDL or security-management command. It is typically outside the application migration core path unless the customer explicitly requires the same workflow.",
            "exclude_from_scope",
            "heuristic_command",
        )
    return (
        "application_api",
        "Medium",
        "Command not explicitly classified. Treat it as application-facing by default and review its migration impact manually.",
        "manual_assessment",
        "heuristic_command",
    )


def _fallback_assessment(usage_row: pd.Series) -> tuple[str, str, str, str, str]:
    feature_type = _clean_text(usage_row.get("feature_type", "")).lower()
    feature_name = _clean_text(usage_row.get("feature_name", ""))
    if feature_type == "command":
        return _infer_command_baseline(feature_name)
    if _looks_like_admin_command(feature_type, feature_name):
        return (
            "ignore",
            "Ignore",
            "This is a MongoDB administrative or operational command and is usually not part of application migration scope.",
            "exclude_from_scope",
            "heuristic_admin",
        )
    return (
        "application_api",
        "Medium",
        "No baseline rule matched; treat this as an application-facing API and review it manually for migration impact.",
        "manual_assessment",
        "fallback",
    )


def _build_complexity_explanation(row: pd.Series) -> str:
    feature_type = _clean_text(row.get("feature_type", ""))
    feature_name = _clean_text(row.get("feature_name", ""))
    scope = _clean_text(row.get("effective_scope", ""))
    complexity = _clean_text(row.get("effective_complexity", ""))
    support = _clean_text(row.get("oracle_support_status", "")) or "Unknown"
    usage_count = pd.to_numeric(row.get("usage_count", 0), errors="coerce")
    usage_text = ""
    if not pd.isna(usage_count) and int(usage_count) > 0:
        usage_text = f" It is observed {int(usage_count)} time(s) in the current workload."

    if scope in {"ignore", "likely_admin"}:
        return (
            f"`{feature_name}` is a {feature_type or 'MongoDB'} feature used mainly for database administration or operations, "
            "not for preserving application behavior during migration to Oracle Database API for MongoDB. "
            "Its migration complexity is therefore Ignore unless the customer explicitly wants to keep this operational workflow."
        )
    if feature_type == "command" and _clean_text(feature_name).lower() in APPLICATION_LOW_COMMANDS:
        command_type = "core application CRUD command"
    elif feature_type == "command" and _clean_text(feature_name).lower() in APPLICATION_MEDIUM_COMMANDS:
        command_type = "application-facing command with semantic differences"
    elif feature_type == "command" and _clean_text(feature_name).lower() in APPLICATION_HIGH_COMMANDS:
        command_type = "application-facing command with complex processing semantics"
    else:
        command_type = f"application-facing {feature_type or 'MongoDB'} feature"
    if support == "Not Supported":
        return (
            f"`{feature_name}` is a {command_type}. "
            "Oracle Database API for MongoDB marks it as Not Supported in the selected target combination, "
            f"so application migration must redesign or replace this behavior. This drives the complexity to {complexity}."
            f"{usage_text}"
        )
    if support == "Partially Supported":
        return (
            f"`{feature_name}` is a {command_type}. "
            "Oracle Database API for MongoDB only partially supports it in the selected target combination, "
            f"so migration requires semantic validation and likely localized rewrite. This results in {complexity} complexity."
            f"{usage_text}"
        )
    if support == "Supported":
        return (
            f"`{feature_name}` is a {command_type} and is supported by "
            "Oracle Database API for MongoDB in the selected target combination. "
            f"Migration complexity remains {complexity} because the application behavior is likely portable with limited change."
            f"{usage_text}"
        )
    return (
        f"`{feature_name}` is a {command_type}, but its support status is currently Unknown "
        "for the selected target combination. Migration complexity is therefore kept conservative until the feature is reviewed manually."
        f"{usage_text}"
    )


def _priority_from_row(row: pd.Series) -> tuple[str, str]:
    complexity = _clean_text(row.get("effective_complexity", "")) or "Medium"
    usage_count_value = pd.to_numeric(row.get("usage_count", 0), errors="coerce")
    max_duration_value = pd.to_numeric(row.get("max_duration_ms", 0), errors="coerce")
    usage_count = int(0 if pd.isna(usage_count_value) else usage_count_value)
    max_duration = int(0 if pd.isna(max_duration_value) else max_duration_value)

    if complexity == "Blocker":
        return "Critical", "Blocker complexity item that requires dedicated remediation."
    if complexity == "High":
        if usage_count >= 10 or max_duration >= 1000:
            return "Critical", "High-complexity API with notable usage or latency."
        return "High", "High-complexity API that likely needs rewrite effort."
    if complexity == "Medium":
        if usage_count >= 20 or max_duration >= 1000:
            return "High", "Medium-complexity API with notable usage or latency."
        return "Medium", "Medium-complexity API requiring semantic validation or small changes."
    if complexity == "Low":
        if usage_count >= 50:
            return "Medium", "Low-complexity API but widely used."
        return "Low", "Low-complexity API with limited migration effort."
    return "Low", "Outside main migration scope."


def assess_migration_complexity(
    usage_df: pd.DataFrame,
    ruleset: MigrationRuleSet,
) -> MigrationAssessmentResult:
    if usage_df.empty:
        empty = usage_df.copy()
        for column in [
            "default_scope",
            "effective_scope",
            "default_migration_necessity",
            "effective_migration_necessity",
            "migration_necessity_reason",
            "scope_confidence",
            "needs_review",
            "default_complexity",
            "effective_complexity",
            "complexity_reason",
            "complexity_explanation",
            "complexity_adjustment_reason",
            "recommended_action",
            "migration_priority",
            "priority_reason",
            "override_applied",
            "rule_source",
            "rules_version",
        ]:
            empty[column] = ""
        return MigrationAssessmentResult(
            detail_df=empty,
            summary_df=pd.DataFrame(columns=["metric", "label", "count", "usage_count", "percentage"]),
            baseline_df=pd.DataFrame(),
            hotspots_df=pd.DataFrame(),
            excluded_df=pd.DataFrame(),
            rules_version=ruleset.rules_version,
            override_count=0,
            rules_coverage_rate=1.0,
            unclassified_feature_count=0,
        )

    detail_df = usage_df.copy()
    baseline_lookup = {
        _normalize_key(row["feature_type"], row["feature_name"]): row
        for _, row in ruleset.baseline_df.iterrows()
    }
    override_lookup = {
        _normalize_key(row["feature_type"], row["feature_name"]): row
        for _, row in ruleset.override_df.iterrows()
    }

    rows: list[dict[str, Any]] = []
    fallback_count = 0
    override_count = 0
    for _, usage_row in detail_df.iterrows():
        usage_dict = usage_row.to_dict()
        key = _normalize_key(usage_row.get("feature_type", ""), usage_row.get("feature_name", ""))
        baseline_row = baseline_lookup.get(key)
        if baseline_row is None:
            default_scope, default_complexity, complexity_reason, recommended_action, rule_source = _fallback_assessment(usage_row)
            if rule_source == "fallback":
                fallback_count += 1
            effective_scope = default_scope
            scope_confidence = "low"
            needs_review = True
            effective_complexity = default_complexity
            review_priority = "P1"
        else:
            default_scope = _clean_text(baseline_row.get("migration_scope", "")) or "application_api"
            effective_scope = default_scope
            scope_confidence = _clean_text(baseline_row.get("scope_confidence", "")) or "medium"
            needs_review = bool(baseline_row.get("needs_review", False))
            default_complexity = _clean_text(baseline_row.get("default_complexity", "")) or "Medium"
            effective_complexity = default_complexity
            complexity_reason = _clean_text(baseline_row.get("complexity_reason", ""))
            recommended_action = _clean_text(baseline_row.get("recommended_action", "")) or "manual_assessment"
            review_priority = _clean_text(baseline_row.get("review_priority", "")) or "P1"
            rule_source = "baseline"

        effective_complexity, complexity_adjustment_reason = _apply_support_based_complexity(
            effective_complexity,
            effective_scope,
            usage_row,
        )

        override_row = override_lookup.get(key)
        override_applied = False
        if override_row is not None:
            override_applied = True
            override_count += 1
            override_scope = _clean_text(override_row.get("override_scope", ""))
            override_complexity = _clean_text(override_row.get("override_complexity", ""))
            override_reason = _clean_text(override_row.get("override_reason", ""))
            override_action = _clean_text(override_row.get("override_action", ""))
            if override_scope:
                effective_scope = override_scope
            if override_complexity:
                effective_complexity = override_complexity
            if override_reason:
                complexity_reason = override_reason
            if override_action:
                recommended_action = override_action

        default_migration_necessity = _migration_necessity(default_scope)
        effective_migration_necessity = _migration_necessity(effective_scope)
        migration_necessity_reason = _migration_necessity_reason(
            effective_scope,
            usage_row.get("feature_name", ""),
        )

        complexity_explanation = _build_complexity_explanation(
            pd.Series(
                {
                    **usage_dict,
                    "feature_type": usage_row.get("feature_type", ""),
                    "feature_name": usage_row.get("feature_name", ""),
                    "effective_scope": effective_scope,
                    "effective_complexity": effective_complexity,
                    "oracle_support_status": usage_row.get("oracle_support_status", ""),
                    "usage_count": usage_row.get("usage_count", 0),
                }
            )
        )

        migration_priority, priority_reason = _priority_from_row(
            pd.Series(
                {
                    "effective_complexity": effective_complexity,
                    "usage_count": usage_row.get("usage_count", 0),
                    "max_duration_ms": usage_row.get("max_duration_ms", 0),
                }
            )
        )

        rows.append(
            {
                **usage_dict,
                "default_scope": default_scope,
                "effective_scope": effective_scope,
                "default_migration_necessity": default_migration_necessity,
                "effective_migration_necessity": effective_migration_necessity,
                "migration_necessity_reason": migration_necessity_reason,
                "scope_confidence": scope_confidence,
                "needs_review": needs_review,
                "default_complexity": default_complexity,
                "effective_complexity": effective_complexity,
                "complexity_reason": complexity_reason,
                "complexity_explanation": complexity_explanation,
                "complexity_adjustment_reason": complexity_adjustment_reason,
                "recommended_action": recommended_action,
                "review_priority": review_priority,
                "migration_priority": migration_priority,
                "priority_reason": priority_reason,
                "override_applied": override_applied,
                "rule_source": rule_source,
                "rules_version": ruleset.rules_version,
            }
        )

    assessed_df = pd.DataFrame(rows)
    assessed_df["_complexity_sort"] = assessed_df["effective_complexity"].map(_complexity_rank)
    assessed_df["_priority_sort"] = assessed_df["migration_priority"].map(PRIORITY_ORDER).fillna(0)
    assessed_df = assessed_df.sort_values(
        by=["_complexity_sort", "_priority_sort", "usage_count", "feature_type", "feature_name"],
        ascending=[False, False, False, True, True],
    ).drop(columns=["_complexity_sort", "_priority_sort"])

    summary_df = build_migration_summary(assessed_df)
    baseline_df = build_api_baseline_assessment(assessed_df)
    hotspots_df = build_migration_hotspots(assessed_df)
    excluded_df = assessed_df[
        assessed_df["effective_scope"].isin(["ignore", "likely_admin"])
    ].copy()
    if not excluded_df.empty:
        excluded_df = excluded_df.sort_values(
            by=["usage_count", "command_name", "feature_name"],
            ascending=[False, True, True],
        ).reset_index(drop=True)

    coverage_denominator = max(len(assessed_df), 1)
    rules_coverage_rate = round((len(assessed_df) - fallback_count) / coverage_denominator, 4)

    return MigrationAssessmentResult(
        detail_df=assessed_df.reset_index(drop=True),
        summary_df=summary_df,
        baseline_df=baseline_df,
        hotspots_df=hotspots_df,
        excluded_df=excluded_df,
        rules_version=ruleset.rules_version,
        override_count=override_count,
        rules_coverage_rate=rules_coverage_rate,
        unclassified_feature_count=fallback_count,
    )


def build_migration_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(columns=["metric", "label", "count", "usage_count", "percentage"])

    app_df = detail_df[detail_df["effective_scope"].eq("application_api")].copy()
    total_features = max(len(app_df), 1)
    total_usage = max(int(app_df["usage_count"].fillna(0).sum()), 1) if not app_df.empty else 1

    frames: list[pd.DataFrame] = []
    complexity_df = (
        detail_df.groupby("effective_complexity", dropna=False)
        .agg(count=("feature_name", "size"), usage_count=("usage_count", "sum"))
        .reset_index()
        .rename(columns={"effective_complexity": "label"})
    )
    complexity_df["metric"] = "effective_complexity"
    complexity_df["percentage"] = (
        complexity_df["count"] / max(len(detail_df), 1) * 100
    ).round(2)
    frames.append(complexity_df)

    scope_df = (
        detail_df.groupby("effective_scope", dropna=False)
        .agg(count=("feature_name", "size"), usage_count=("usage_count", "sum"))
        .reset_index()
        .rename(columns={"effective_scope": "label"})
    )
    scope_df["metric"] = "effective_scope"
    scope_df["percentage"] = (
        scope_df["count"] / max(len(detail_df), 1) * 100
    ).round(2)
    frames.append(scope_df)

    if "effective_migration_necessity" in detail_df.columns:
        necessity_df = (
            detail_df.groupby("effective_migration_necessity", dropna=False)
            .agg(count=("feature_name", "size"), usage_count=("usage_count", "sum"))
            .reset_index()
            .rename(columns={"effective_migration_necessity": "label"})
        )
        necessity_df["metric"] = "effective_migration_necessity"
        necessity_df["percentage"] = (
            necessity_df["count"] / max(len(detail_df), 1) * 100
        ).round(2)
        frames.append(necessity_df)

    priority_df = (
        app_df.groupby("migration_priority", dropna=False)
        .agg(count=("feature_name", "size"), usage_count=("usage_count", "sum"))
        .reset_index()
        .rename(columns={"migration_priority": "label"})
    )
    priority_df["metric"] = "migration_priority"
    priority_df["percentage"] = (
        priority_df["usage_count"] / total_usage * 100
    ).round(2)
    frames.append(priority_df)

    status_df = (
        app_df.groupby("oracle_support_status", dropna=False)
        .agg(count=("feature_name", "size"), usage_count=("usage_count", "sum"))
        .reset_index()
        .rename(columns={"oracle_support_status": "label"})
    )
    status_df["metric"] = "oracle_support_status"
    status_df["percentage"] = (
        status_df["count"] / total_features * 100
    ).round(2)
    frames.append(status_df)

    return pd.concat(frames, ignore_index=True)[
        ["metric", "label", "count", "usage_count", "percentage"]
    ]


def build_migration_hotspots(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()

    hotspots_df = detail_df[
        detail_df["effective_scope"].eq("application_api")
        & (
            detail_df["effective_complexity"].isin(["High", "Blocker"])
            | (
                detail_df["effective_complexity"].eq("Medium")
                & pd.to_numeric(detail_df["usage_count"], errors="coerce").fillna(0).ge(20)
            )
        )
    ].copy()
    if hotspots_df.empty:
        return hotspots_df
    return hotspots_df.sort_values(
        by=["migration_priority", "effective_complexity", "usage_count", "max_duration_ms"],
        ascending=[False, False, False, False],
        key=lambda col: col.map(PRIORITY_ORDER) if col.name == "migration_priority" else (
            col.map(COMPLEXITY_ORDER) if col.name == "effective_complexity" else col
        ),
    ).reset_index(drop=True)


def build_api_baseline_assessment(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()

    grouped = (
        detail_df.groupby(
            ["feature_type", "feature_name", "oracle_category", "oracle_support_since"],
            dropna=False,
            as_index=False,
        )
        .agg(
            command_contexts=("command_name", lambda values: ", ".join(sorted({str(v) for v in values if str(v).strip()}))),
            usage_count=("usage_count", "sum"),
            collections=("collection", lambda values: ", ".join(sorted({str(v) for v in values if str(v).strip()}))),
            database_count=("database", lambda values: len({str(v) for v in values if str(v).strip()})),
            default_scope=("default_scope", "first"),
            effective_scope=("effective_scope", "first"),
            default_migration_necessity=("default_migration_necessity", "first"),
            effective_migration_necessity=("effective_migration_necessity", "first"),
            migration_necessity_reason=("migration_necessity_reason", "first"),
            default_complexity=("default_complexity", "first"),
            effective_complexity=("effective_complexity", "first"),
            recommended_action=("recommended_action", "first"),
            complexity_reason=("complexity_reason", "first"),
            complexity_explanation=("complexity_explanation", "first"),
            override_applied=("override_applied", "max"),
            oracle_support_statuses=("oracle_support_status", lambda values: ", ".join(sorted({str(v) for v in values if str(v).strip()}))),
        )
        .rename(columns={"oracle_category": "oracle_section"})
        .sort_values(
            by=["usage_count", "feature_type", "feature_name", "oracle_section"],
            ascending=[False, True, True, True],
        )
        .reset_index(drop=True)
    )
    grouped["override_scope"] = ""
    grouped["override_complexity"] = ""
    grouped["override_action"] = ""
    grouped["override_reason"] = ""
    grouped["override_enabled"] = False
    return grouped
