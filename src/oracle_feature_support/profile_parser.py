from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
from typing import Any

import pandas as pd


KNOWN_COMMAND_SEQUENCE = (
    "find",
    "aggregate",
    "insert",
    "update",
    "delete",
    "findAndModify",
    "distinct",
    "count",
    "createIndexes",
    "dropIndexes",
    "listIndexes",
)
KNOWN_COMMANDS = set(KNOWN_COMMAND_SEQUENCE)

SUPPORTED_STAGES = {
    "$match",
    "$project",
    "$group",
    "$sort",
    "$limit",
    "$skip",
    "$unwind",
    "$lookup",
    "$facet",
    "$count",
    "$addFields",
    "$set",
    "$unset",
    "$merge",
    "$out",
    "$bucketAuto",
    "$graphLookup",
}

SUPPORTED_OPERATORS = {
    "$in",
    "$nin",
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$exists",
    "$regex",
    "$and",
    "$or",
    "$not",
    "$nor",
    "$elemMatch",
    "$size",
    "$all",
    "$set",
    "$unset",
    "$inc",
    "$push",
    "$pull",
    "$addToSet",
    "$rename",
    "$min",
    "$max",
}

SUPPORTED_EXPRESSIONS = {
    "$sum",
    "$avg",
    "$min",
    "$max",
    "$cond",
    "$ifNull",
    "$map",
    "$filter",
    "$reduce",
    "$concat",
    "$substr",
    "$toString",
    "$dateToString",
    "$year",
    "$month",
    "$dayOfMonth",
    "$regexMatch",
    "$setField",
}

EXPRESSION_STAGE_NAMES = {"$project", "$group", "$addFields", "$set"}


@dataclass
class ProfileEvent:
    ts: str
    db_name: str
    collection_name: str
    op: str
    command_name: str
    command_doc: dict[str, Any]
    duration_ms: int | None
    docs_examined: int | None
    keys_examined: int | None
    nreturned: int | None
    err_code: int | None
    err_msg: str | None
    raw: dict[str, Any]


@dataclass
class FeatureUsage:
    feature_type: str
    feature_name: str
    command_name: str
    op_type: str
    database: str
    collection: str
    event_ts: str
    duration_ms: int | None
    sample_path: str
    sample_value: str


def _to_iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "")


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def _split_namespace(ns: str) -> tuple[str, str]:
    if not ns or "." not in ns:
        return "", ""
    db_name, collection_name = ns.split(".", 1)
    return db_name, collection_name


def _detect_command_name(doc: dict[str, Any], op: str) -> str:
    for key in KNOWN_COMMAND_SEQUENCE:
        if key in doc:
            return key
    fallback_map = {
        "query": "find",
        "insert": "insert",
        "update": "update",
        "remove": "delete",
        "command": "unknown",
    }
    return fallback_map.get(str(op or "").strip(), "unknown")


def normalize_profile_records(records: list[dict[str, Any]]) -> list[ProfileEvent]:
    events: list[ProfileEvent] = []
    for record in records:
        raw_command = record.get("command")
        if not isinstance(raw_command, dict):
            raw_command = {}
        ns = str(record.get("ns", "") or "")
        db_name, collection_name = _split_namespace(ns)
        if not db_name:
            db_name = str(record.get("db", "") or "")
        event = ProfileEvent(
            ts=_to_iso(record.get("ts")),
            db_name=db_name,
            collection_name=collection_name,
            op=str(record.get("op", "") or ""),
            command_name=_detect_command_name(raw_command, str(record.get("op", "") or "")),
            command_doc=raw_command,
            duration_ms=_to_int(record.get("millis")),
            docs_examined=_to_int(record.get("docsExamined")),
            keys_examined=_to_int(record.get("keysExamined")),
            nreturned=_to_int(record.get("nreturned")),
            err_code=_to_int(record.get("errCode")),
            err_msg=str(record.get("errMsg", "") or "") or None,
            raw=record,
        )
        events.append(event)
    return events


def _emit_feature(
    target: list[FeatureUsage],
    event: ProfileEvent,
    feature_type: str,
    feature_name: str,
    sample_path: str,
    sample_value: Any,
) -> None:
    target.append(
        FeatureUsage(
            feature_type=feature_type,
            feature_name=feature_name,
            command_name=event.command_name,
            op_type=event.op,
            database=event.db_name,
            collection=event.collection_name,
            event_ts=event.ts,
            duration_ms=event.duration_ms,
            sample_path=sample_path,
            sample_value=_json_dumps(sample_value),
        )
    )


def _walk_expression(
    value: Any,
    path: str,
    event: ProfileEvent,
    target: list[FeatureUsage],
) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else key
            if key in SUPPORTED_EXPRESSIONS:
                _emit_feature(target, event, "expression", key, child_path, child)
            if key.startswith("$"):
                _walk_expression(child, child_path, event, target)
            else:
                _walk_expression(child, child_path, event, target)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _walk_expression(item, f"{path}[{index}]", event, target)


def _walk_operator(
    value: Any,
    path: str,
    event: ProfileEvent,
    target: list[FeatureUsage],
) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else key
            if key == "$expr":
                _emit_feature(target, event, "operator", key, child_path, child)
                _walk_expression(child, child_path, event, target)
                continue
            if key in SUPPORTED_OPERATORS:
                _emit_feature(target, event, "operator", key, child_path, child)
            _walk_operator(child, child_path, event, target)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _walk_operator(item, f"{path}[{index}]", event, target)


def _extract_find_features(event: ProfileEvent, target: list[FeatureUsage]) -> None:
    command = event.command_doc
    if "filter" in command:
        _walk_operator(command.get("filter"), "command.filter", event, target)
    if "projection" in command:
        _walk_operator(command.get("projection"), "command.projection", event, target)


def _extract_update_features(event: ProfileEvent, target: list[FeatureUsage]) -> None:
    updates = event.command_doc.get("updates")
    if isinstance(updates, list):
        for index, item in enumerate(updates):
            if not isinstance(item, dict):
                continue
            _walk_operator(item.get("q"), f"command.updates[{index}].q", event, target)
            _walk_operator(item.get("u"), f"command.updates[{index}].u", event, target)


def _extract_delete_features(event: ProfileEvent, target: list[FeatureUsage]) -> None:
    deletes = event.command_doc.get("deletes")
    if isinstance(deletes, list):
        for index, item in enumerate(deletes):
            if not isinstance(item, dict):
                continue
            _walk_operator(item.get("q"), f"command.deletes[{index}].q", event, target)


def _extract_find_and_modify_features(event: ProfileEvent, target: list[FeatureUsage]) -> None:
    command = event.command_doc
    if "query" in command:
        _walk_operator(command.get("query"), "command.query", event, target)
    if "update" in command:
        _walk_operator(command.get("update"), "command.update", event, target)


def _extract_aggregate_features(event: ProfileEvent, target: list[FeatureUsage]) -> None:
    pipeline = event.command_doc.get("pipeline")
    if not isinstance(pipeline, list):
        return
    for index, stage in enumerate(pipeline):
        if not isinstance(stage, dict) or not stage:
            continue
        stage_name = next(iter(stage))
        stage_body = stage.get(stage_name)
        stage_path = f"command.pipeline[{index}].{stage_name}"
        if stage_name in SUPPORTED_STAGES:
            _emit_feature(target, event, "stage", stage_name, stage_path, stage_body)
        if stage_name == "$match":
            _walk_operator(stage_body, stage_path, event, target)
        elif stage_name in EXPRESSION_STAGE_NAMES:
            _walk_expression(stage_body, stage_path, event, target)
        elif stage_name == "$lookup" and isinstance(stage_body, dict):
            lookup_pipeline = stage_body.get("pipeline")
            if isinstance(lookup_pipeline, list):
                nested_event = ProfileEvent(**asdict(event))
                nested_event.command_name = event.command_name
                _extract_aggregate_features(
                    ProfileEvent(
                        **{
                            **asdict(nested_event),
                            "command_doc": {"pipeline": lookup_pipeline},
                        }
                    ),
                    target,
                )
        elif stage_name == "$facet" and isinstance(stage_body, dict):
            for facet_name, facet_pipeline in stage_body.items():
                if isinstance(facet_pipeline, list):
                    _extract_aggregate_features(
                        ProfileEvent(
                            **{
                                **asdict(event),
                                "command_doc": {"pipeline": facet_pipeline},
                            }
                        ),
                        target,
                    )
        else:
            _walk_expression(stage_body, stage_path, event, target)


def extract_feature_usages(events: list[ProfileEvent]) -> pd.DataFrame:
    features: list[FeatureUsage] = []
    for event in events:
        if event.command_name in KNOWN_COMMANDS:
            _emit_feature(
                features,
                event,
                "command",
                event.command_name,
                f"command.{event.command_name}",
                event.command_doc.get(event.command_name),
            )

        if event.command_name == "find":
            _extract_find_features(event, features)
        elif event.command_name == "update":
            _extract_update_features(event, features)
        elif event.command_name == "delete":
            _extract_delete_features(event, features)
        elif event.command_name == "findAndModify":
            _extract_find_and_modify_features(event, features)
        elif event.command_name == "aggregate":
            _extract_aggregate_features(event, features)

    if not features:
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
            ]
        )

    df = pd.DataFrame(asdict(feature) for feature in features)
    grouped = (
        df.groupby(
            ["feature_type", "feature_name", "command_name", "op_type", "database", "collection"],
            dropna=False,
            as_index=False,
        )
        .agg(
            usage_count=("feature_name", "size"),
            first_seen=("event_ts", "min"),
            last_seen=("event_ts", "max"),
            max_duration_ms=("duration_ms", "max"),
            sample_path=("sample_path", "first"),
            sample_value=("sample_value", "first"),
        )
        .sort_values(
            by=["usage_count", "feature_type", "feature_name", "database", "collection", "op_type"],
            ascending=[False, True, True, True, True, True],
        )
        .reset_index(drop=True)
    )
    return grouped


def events_to_dataframe(events: list[ProfileEvent]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(
            columns=[
                "ts",
                "db_name",
                "collection_name",
                "op",
                "command_name",
                "duration_ms",
                "docs_examined",
                "keys_examined",
                "nreturned",
                "err_code",
                "err_msg",
            ]
        )
    return pd.DataFrame(
        {
            "ts": event.ts,
            "db_name": event.db_name,
            "collection_name": event.collection_name,
            "op": event.op,
            "command_name": event.command_name,
            "duration_ms": event.duration_ms,
            "docs_examined": event.docs_examined,
            "keys_examined": event.keys_examined,
            "nreturned": event.nreturned,
            "err_code": event.err_code,
            "err_msg": event.err_msg or "",
        }
        for event in events
    )
