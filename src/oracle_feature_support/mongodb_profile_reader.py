from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import re
from typing import Any, Callable

from pymongo import MongoClient
from pymongo.errors import PyMongoError

SYSTEM_DATABASES = {"admin", "config", "local"}


PROFILE_PROJECTION = {
    "ts": 1,
    "ns": 1,
    "op": 1,
    "command": 1,
    "query": 1,
    "updateobj": 1,
    "millis": 1,
    "nreturned": 1,
    "docsExamined": 1,
    "keysExamined": 1,
    "errCode": 1,
    "errMsg": 1,
}


@dataclass
class ProfileReadResult:
    records: list[dict[str, Any]]
    truncated: bool
    fetched_at: str


@dataclass
class MongoConnectionTestResult:
    ok: bool
    fetched_at: str
    database_name: str
    available_databases: list[str]
    has_system_profile: bool
    collection_count: int
    sample_collections: list[str]
    can_read_logs: bool
    can_read_server_status_metrics: bool
    capability_notes: list[str]


@dataclass
class MetricsReadResult:
    rows: list[dict[str, Any]]
    fetched_at: str
    filtered_row_count: int
    dropped_command_names: list[str]


@dataclass
class LogReadResult:
    rows: list[dict[str, Any]]
    fetched_at: str
    scanned_line_count: int
    matched_line_count: int


METRICS_NOISE_COMMANDS = {
    "<unknown>",
    "authenticate",
    "buildinfo",
    "collstats",
    "connectionstatus",
    "dbstats",
    "endsessions",
    "getlog",
    "getparameter",
    "getshardmap",
    "getdefaultrwconcern",
    "grantrolestouser",
    "hello",
    "hostinfo",
    "ismaster",
    "listcollections",
    "listdatabases",
    "listindexes",
    "ping",
    "profile",
    "saslcontinue",
    "saslstart",
    "serverstatus",
}

COMMAND_LOG_REGEX = re.compile(r'"c"\s*:\s*"COMMAND"')
COMMAND_NAME_REGEX = re.compile(r'"([A-Za-z][A-Za-z0-9_]*)"\s*:')


def _safe_admin_command(
    client: MongoClient,
    command: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        result = client.admin.command(command)
        return result if isinstance(result, dict) else {}, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def collect_instance_inventory(
    mongodb_uri: str,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    client = None
    try:
        if progress_callback:
            progress_callback("[MONGO] Collect instance inventory")
        client = MongoClient(mongodb_uri, tz_aware=True, serverSelectionTimeoutMS=8000)
        ping_result = client.admin.command("ping")
        hello_result, hello_error = _safe_admin_command(client, {"hello": 1})
        build_info_result, build_info_error = _safe_admin_command(client, {"buildInfo": 1})
        host_info_result, host_info_error = _safe_admin_command(client, {"hostInfo": 1})
        list_databases_result, list_databases_error = _safe_admin_command(client, {"listDatabases": 1})
        rw_concern_result, rw_concern_error = _safe_admin_command(client, {"getDefaultRWConcern": 1})
        server_status_result, server_status_error = _safe_admin_command(client, {"serverStatus": 1})
        shard_map_result, shard_map_error = _safe_admin_command(client, {"getShardMap": 1})

        databases: list[dict[str, Any]] = []
        raw_databases = (list_databases_result or {}).get("databases", [])
        if isinstance(raw_databases, list):
            for item in raw_databases:
                if not isinstance(item, dict):
                    continue
                database_name = str(item.get("name", "") or "").strip()
                if not database_name:
                    continue
                databases.append(
                    {
                        "database": database_name,
                        "sizeOnDisk": item.get("sizeOnDisk"),
                        "empty": bool(item.get("empty", False)),
                        "is_system": database_name.lower() in SYSTEM_DATABASES,
                    }
                )

        shard_map = shard_map_result or {}
        shard_names = []
        map_value = shard_map.get("map")
        if isinstance(map_value, dict):
            shard_names = sorted(str(key) for key in map_value.keys() if str(key).strip())
        elif isinstance(shard_map.get("hosts"), dict):
            shard_names = sorted(str(key) for key in shard_map.get("hosts", {}).keys() if str(key).strip())

        topology_type = ""
        if hello_result:
            if hello_result.get("msg") == "isdbgrid":
                topology_type = "mongos"
            elif hello_result.get("setName"):
                topology_type = "replicaSet"
            else:
                topology_type = "standalone"

        server_status = server_status_result or {}
        connections = server_status.get("connections") if isinstance(server_status.get("connections"), dict) else {}
        metrics = server_status.get("metrics") if isinstance(server_status.get("metrics"), dict) else {}
        mem = server_status.get("mem") if isinstance(server_status.get("mem"), dict) else {}
        storage_engine = ""
        if isinstance(server_status.get("storageEngine"), dict):
            storage_engine = str(server_status.get("storageEngine", {}).get("name", "") or "")

        host_info = host_info_result or {}
        system_info = host_info.get("system") if isinstance(host_info.get("system"), dict) else {}
        os_info = system_info.get("os") if isinstance(system_info.get("os"), dict) else {}

        inventory = {
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "ping_ok": bool((ping_result or {}).get("ok") == 1.0),
            "topology_type": topology_type,
            "hello": {
                "maxWireVersion": (hello_result or {}).get("maxWireVersion"),
                "setName": (hello_result or {}).get("setName", ""),
                "isWritablePrimary": bool((hello_result or {}).get("isWritablePrimary", False)),
                "secondary": bool((hello_result or {}).get("secondary", False)),
                "msg": (hello_result or {}).get("msg", ""),
                "hosts": (hello_result or {}).get("hosts", []) if isinstance((hello_result or {}).get("hosts"), list) else [],
            },
            "buildInfo": {
                "version": (build_info_result or {}).get("version", ""),
                "gitVersion": (build_info_result or {}).get("gitVersion", ""),
                "modules": (build_info_result or {}).get("modules", []) if isinstance((build_info_result or {}).get("modules"), list) else [],
                "allocator": (build_info_result or {}).get("allocator", ""),
            },
            "hostInfo": {
                "hostname": system_info.get("hostname", ""),
                "cpuCores": system_info.get("numCores"),
                "memSizeMB": system_info.get("memSizeMB"),
                "cpuArch": system_info.get("cpuArch", ""),
                "osName": os_info.get("name", ""),
                "osVersion": os_info.get("version", ""),
            },
            "defaultRWConcern": {
                "defaultReadConcern": (rw_concern_result or {}).get("defaultReadConcern", {}),
                "defaultWriteConcern": (rw_concern_result or {}).get("defaultWriteConcern", {}),
            },
            "serverStatus": {
                "uptime": server_status.get("uptime"),
                "connectionsCurrent": connections.get("current"),
                "connectionsAvailable": connections.get("available"),
                "storageEngine": storage_engine,
                "residentMemoryMB": mem.get("resident"),
                "virtualMemoryMB": mem.get("virtual"),
                "commandMetricCount": len(metrics.get("commands", {})) if isinstance(metrics.get("commands"), dict) else 0,
                "aggStageMetricCount": len(metrics.get("aggStageCounters", {})) if isinstance(metrics.get("aggStageCounters"), dict) else 0,
                "operatorMetricCount": len(metrics.get("operatorCounters", {})) if isinstance(metrics.get("operatorCounters"), dict) else 0,
            },
            "listDatabases": databases,
            "sharding": {
                "hasShardMap": bool(shard_names),
                "shardCount": len(shard_names),
                "shardNames": shard_names,
            },
            "errors": {
                "hello": hello_error,
                "buildInfo": build_info_error,
                "hostInfo": host_info_error,
                "listDatabases": list_databases_error,
                "getDefaultRWConcern": rw_concern_error,
                "serverStatus": server_status_error,
                "getShardMap": shard_map_error,
            },
        }
        return inventory
    finally:
        if client is not None:
            client.close()


def read_system_profile(
    mongodb_uri: str,
    database_name: str,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int = 20_000,
    progress_callback: Callable[[str], None] | None = None,
) -> ProfileReadResult:
    client = None
    try:
        if progress_callback:
            progress_callback(f"[MONGO] Connect -> {database_name}")
        client = MongoClient(mongodb_uri, tz_aware=True, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")

        database = client[database_name]
        collection_names = set(database.list_collection_names())
        if "system.profile" not in collection_names:
            raise ValueError(
                f"数据库 {database_name} 未发现 system.profile，请先确认已开启 profiler。"
            )

        query: dict[str, Any] = {}
        ts_filter: dict[str, Any] = {}
        if start_time is not None:
            ts_filter["$gte"] = start_time
        if end_time is not None:
            ts_filter["$lte"] = end_time
        if ts_filter:
            query["ts"] = ts_filter
        # Exclude profiler reads against system.profile itself so the analysis
        # does not count its own collection queries as application workload.
        query["ns"] = {"$ne": f"{database_name}.system.profile"}

        if progress_callback:
            progress_callback(
                f"[MONGO] Query system.profile window={query.get('ts', '{}')} limit={limit}"
            )

        cursor = (
            database["system.profile"]
            .find(query, PROFILE_PROJECTION)
            .sort("ts", -1)
            .limit(int(limit) + 1)
        )
        docs = list(cursor)
        truncated = len(docs) > int(limit)
        if truncated:
            docs = docs[: int(limit)]

        if progress_callback:
            progress_callback(
                f"[MONGO] Retrieved {len(docs)} profile document(s)"
                + (" (truncated)" if truncated else "")
            )

        return ProfileReadResult(
            records=docs,
            truncated=truncated,
            fetched_at=datetime.now().isoformat(timespec="seconds"),
        )
    except PyMongoError as exc:
        raise RuntimeError(f"MongoDB 读取失败: {exc}") from exc
    finally:
        if client is not None:
            client.close()


def test_mongodb_connection(
    mongodb_uri: str,
    database_name: str,
    progress_callback: Callable[[str], None] | None = None,
) -> MongoConnectionTestResult:
    client = None
    try:
        if progress_callback:
            progress_callback(f"[MONGO] Connect test -> {database_name}")
        client = MongoClient(mongodb_uri, tz_aware=True, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        normalized_database_name = str(database_name or "").strip()
        all_database_names = sorted(client.list_database_names())
        available_databases = [
            name for name in all_database_names
            if str(name or "").strip().lower() not in SYSTEM_DATABASES
        ]
        collection_names: list[str] = []
        has_system_profile = False
        if normalized_database_name:
            database = client[normalized_database_name]
            collection_names = sorted(database.list_collection_names())
            has_system_profile = "system.profile" in set(collection_names)
        can_read_logs = False
        can_read_server_status_metrics = False
        capability_notes: list[str] = []

        try:
            client.admin.command({"getLog": "global"})
            can_read_logs = True
        except PyMongoError as exc:
            capability_notes.append(f"logs unavailable: {exc}")

        try:
            server_status = client.admin.command({"serverStatus": 1})
            metrics = server_status.get("metrics")
            can_read_server_status_metrics = isinstance(metrics, dict)
            if not can_read_server_status_metrics:
                capability_notes.append("serverStatus.metrics unavailable")
        except PyMongoError as exc:
            capability_notes.append(f"serverStatus unavailable: {exc}")

        if progress_callback:
            progress_callback(
                f"[MONGO] Ping ok, dbs={len(available_databases)}, collections={len(collection_names)}, system.profile={has_system_profile}, logs={can_read_logs}, metrics={can_read_server_status_metrics}"
            )
        return MongoConnectionTestResult(
            ok=True,
            fetched_at=datetime.now().isoformat(timespec="seconds"),
            database_name=normalized_database_name,
            available_databases=available_databases,
            has_system_profile=has_system_profile,
            collection_count=len(collection_names),
            sample_collections=collection_names[:20],
            can_read_logs=can_read_logs,
            can_read_server_status_metrics=can_read_server_status_metrics,
            capability_notes=capability_notes,
        )
    except PyMongoError as exc:
        raise RuntimeError(f"MongoDB 连接测试失败: {exc}") from exc
    finally:
        if client is not None:
            client.close()


def _extract_metric_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, dict):
        if isinstance(value.get("total"), (int, float)):
            return int(value.get("total"))
        numeric_values = [
            int(item)
            for item in value.values()
            if isinstance(item, (int, float)) and not isinstance(item, bool)
        ]
        if numeric_values:
            return sum(numeric_values)
    return None


def _extract_stage_counter_rows(
    value: Any,
    path: str,
    database_name: str,
    rows: list[dict[str, Any]],
) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            if str(key).startswith("$"):
                count = _extract_metric_count(child)
                if count and count > 0:
                    rows.append(
                        {
                            "feature_type": "stage",
                            "feature_name": str(key),
                            "command_name": "aggregate",
                            "op_type": "command",
                            "database": database_name,
                            "collection": "",
                            "usage_count": count,
                            "first_seen": "",
                            "last_seen": "",
                            "max_duration_ms": None,
                            "sample_path": child_path,
                            "sample_value": str(child),
                        }
                    )
            _extract_stage_counter_rows(child, child_path, database_name, rows)


def _extract_operator_counter_rows(
    value: Any,
    path: str,
    database_name: str,
    rows: list[dict[str, Any]],
) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            if str(key).startswith("$"):
                count = _extract_metric_count(child)
                if count and count > 0:
                    rows.append(
                        {
                            "feature_type": "operator",
                            "feature_name": str(key),
                            "command_name": "unknown",
                            "op_type": "command",
                            "database": database_name,
                            "collection": "",
                            "usage_count": count,
                            "first_seen": "",
                            "last_seen": "",
                            "max_duration_ms": None,
                            "sample_path": child_path,
                            "sample_value": str(child),
                        }
                    )
            _extract_operator_counter_rows(child, child_path, database_name, rows)


def read_server_status_metrics(
    mongodb_uri: str,
    database_name: str,
    filter_noise_commands: bool = False,
    progress_callback: Callable[[str], None] | None = None,
) -> MetricsReadResult:
    client = None
    try:
        if progress_callback:
            progress_callback(f"[MONGO] Connect metrics -> {database_name}")
        client = MongoClient(mongodb_uri, tz_aware=True, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        server_status = client.admin.command({"serverStatus": 1})
        metrics = server_status.get("metrics")
        if not isinstance(metrics, dict):
            raise ValueError("当前实例未返回 serverStatus.metrics。")

        rows: list[dict[str, Any]] = []

        commands = metrics.get("commands")
        dropped_command_names: list[str] = []
        filtered_row_count = 0

        if isinstance(commands, dict):
            for command_name, command_value in commands.items():
                normalized_command_name = str(command_name).strip().lower()
                if filter_noise_commands and normalized_command_name in METRICS_NOISE_COMMANDS:
                    dropped_command_names.append(str(command_name))
                    filtered_row_count += 1
                    continue
                count = _extract_metric_count(command_value)
                if count and count > 0:
                    rows.append(
                        {
                            "feature_type": "command",
                            "feature_name": str(command_name),
                            "command_name": str(command_name),
                            "op_type": "command",
                            "database": database_name,
                            "collection": "",
                            "usage_count": count,
                            "first_seen": "",
                            "last_seen": "",
                            "max_duration_ms": None,
                            "sample_path": f"metrics.commands.{command_name}",
                            "sample_value": str(command_value),
                        }
                    )

        agg_stage_counters = metrics.get("aggStageCounters")
        if isinstance(agg_stage_counters, dict):
            _extract_stage_counter_rows(
                agg_stage_counters,
                "metrics.aggStageCounters",
                database_name,
                rows,
            )

        operator_counters = metrics.get("operatorCounters")
        if isinstance(operator_counters, dict):
            _extract_operator_counter_rows(
                operator_counters,
                "metrics.operatorCounters",
                database_name,
                rows,
            )

        if progress_callback:
            progress_callback(
                f"[MONGO] Retrieved {len(rows)} metric usage row(s), filtered={filtered_row_count}, filter_noise={filter_noise_commands}"
            )

        return MetricsReadResult(
            rows=rows,
            fetched_at=datetime.now().isoformat(timespec="seconds"),
            filtered_row_count=filtered_row_count,
            dropped_command_names=sorted(set(dropped_command_names)),
        )
    except PyMongoError as exc:
        raise RuntimeError(f"MongoDB metrics 读取失败: {exc}") from exc
    finally:
        if client is not None:
            client.close()


def _extract_command_name_from_log_doc(doc: dict[str, Any]) -> str:
    attr = doc.get("attr")
    if isinstance(attr, dict):
        command = attr.get("command")
        if isinstance(command, dict):
            for key in command:
                normalized = str(key or "").strip()
                if normalized and normalized not in {"$db", "lsid"} and not normalized.startswith("$"):
                    return normalized
    raw = json.dumps(doc, ensure_ascii=False)
    match = COMMAND_NAME_REGEX.search(raw)
    return match.group(1) if match else "unknown"


def read_global_log(
    mongodb_uri: str,
    database_name: str,
    progress_callback: Callable[[str], None] | None = None,
) -> LogReadResult:
    client = None
    try:
        if progress_callback:
            progress_callback(f"[MONGO] Connect logs -> {database_name}")
        client = MongoClient(mongodb_uri, tz_aware=True, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        response = client.admin.command({"getLog": "global"})
        lines = response.get("log")
        if not isinstance(lines, list):
            raise ValueError("当前实例未返回可解析的 global 日志。")

        rows: list[dict[str, Any]] = []
        matched_line_count = 0
        for raw in lines:
            if not isinstance(raw, str):
                continue
            if database_name not in raw:
                continue
            if not COMMAND_LOG_REGEX.search(raw) and "Slow query" not in raw:
                continue
            matched_line_count += 1
            try:
                doc = json.loads(raw)
            except Exception:
                continue

            attr = doc.get("attr")
            if not isinstance(attr, dict):
                continue
            ns = str(attr.get("ns", "") or "")
            if not ns or not ns.startswith(f"{database_name}."):
                continue
            command_name = _extract_command_name_from_log_doc(doc)
            rows.append(
                {
                    "feature_type": "command",
                    "feature_name": command_name,
                    "command_name": command_name,
                    "op_type": "command",
                    "database": database_name,
                    "collection": ns.split(".", 1)[1] if "." in ns else "",
                    "usage_count": 1,
                    "first_seen": str(doc.get("t", {}).get("$date", "") or ""),
                    "last_seen": str(doc.get("t", {}).get("$date", "") or ""),
                    "max_duration_ms": None,
                    "sample_path": "log.command",
                    "sample_value": raw,
                }
            )

        if progress_callback:
            progress_callback(
                f"[MONGO] Retrieved {len(rows)} log-derived usage row(s), scanned={len(lines)}, matched={matched_line_count}"
            )

        return LogReadResult(
            rows=rows,
            fetched_at=datetime.now().isoformat(timespec="seconds"),
            scanned_line_count=len(lines),
            matched_line_count=matched_line_count,
        )
    except PyMongoError as exc:
        raise RuntimeError(f"MongoDB 日志读取失败: {exc}") from exc
    finally:
        if client is not None:
            client.close()
