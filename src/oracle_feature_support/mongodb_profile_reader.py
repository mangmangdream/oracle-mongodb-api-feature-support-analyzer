from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from pymongo import MongoClient
from pymongo.errors import PyMongoError


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
    has_system_profile: bool
    collection_count: int
    sample_collections: list[str]


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
        database = client[database_name]
        collection_names = sorted(database.list_collection_names())
        has_system_profile = "system.profile" in set(collection_names)
        if progress_callback:
            progress_callback(
                f"[MONGO] Ping ok, collections={len(collection_names)}, system.profile={has_system_profile}"
            )
        return MongoConnectionTestResult(
            ok=True,
            fetched_at=datetime.now().isoformat(timespec="seconds"),
            database_name=database_name,
            has_system_profile=has_system_profile,
            collection_count=len(collection_names),
            sample_collections=collection_names[:20],
        )
    except PyMongoError as exc:
        raise RuntimeError(f"MongoDB 连接测试失败: {exc}") from exc
    finally:
        if client is not None:
            client.close()
