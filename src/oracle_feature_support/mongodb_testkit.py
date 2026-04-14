from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import PyMongoError


ProgressCallback = Callable[[str], None] | None


@dataclass
class MongoSeedResult:
    database_name: str
    finished_at: str
    dropped_collections: list[str]
    inserted_counts: dict[str, int]
    created_indexes: dict[str, list[str]]


@dataclass
class MongoExerciseResult:
    database_name: str
    finished_at: str
    command_count: int
    original_profile_level: int
    restored_profile_level: int
    profile_count_before: int
    profile_count_after: int
    unsupported_feature_names: list[str]


def _emit(progress_callback: ProgressCallback, message: str) -> None:
    if progress_callback:
        progress_callback(message)


def seed_test_data(
    mongodb_uri: str,
    database_name: str,
    progress_callback: ProgressCallback = None,
) -> MongoSeedResult:
    client = None
    try:
        _emit(progress_callback, f"[TESTKIT] Connect seed -> {database_name}")
        client = MongoClient(mongodb_uri, tz_aware=True, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        database = client[database_name]

        dropped_collections: list[str] = []
        for collection_name in ["customers", "orders", "inventory", "order_metrics", "order_archive"]:
            database[collection_name].drop()
            dropped_collections.append(collection_name)
        _emit(progress_callback, "[TESTKIT] Dropped target collections")

        customers = [
            {
                "_id": 1,
                "customerCode": "C001",
                "referrerCode": None,
                "name": "Alice Chen",
                "tier": "gold",
                "region": "SH",
                "tags": ["vip", "b2b"],
                "email": "alice.chen@example.com",
                "createdAt": datetime.fromisoformat("2026-03-01T09:00:00+00:00"),
            },
            {
                "_id": 2,
                "customerCode": "C002",
                "referrerCode": "C001",
                "name": "Bob Wang",
                "tier": "silver",
                "region": "BJ",
                "tags": ["retail"],
                "email": "bob.wang@example.com",
                "createdAt": datetime.fromisoformat("2026-03-03T10:15:00+00:00"),
            },
            {
                "_id": 3,
                "customerCode": "C003",
                "referrerCode": "C001",
                "name": "Cathy Liu",
                "tier": "gold",
                "region": "SZ",
                "tags": ["vip", "online"],
                "email": None,
                "createdAt": datetime.fromisoformat("2026-03-05T11:30:00+00:00"),
            },
            {
                "_id": 4,
                "customerCode": "C004",
                "referrerCode": "C003",
                "name": "David Sun",
                "tier": "bronze",
                "region": "HZ",
                "tags": ["online"],
                "createdAt": datetime.fromisoformat("2026-03-08T08:20:00+00:00"),
            },
        ]
        inventory = [
            {"_id": 101, "sku": "SKU-1001", "category": "book", "price": 88, "warehouse": "WH1", "stock": 120, "tags": ["paper", "sale"]},
            {"_id": 102, "sku": "SKU-1002", "category": "book", "price": 128, "warehouse": "WH2", "stock": 80, "tags": ["paper"]},
            {"_id": 103, "sku": "SKU-2001", "category": "device", "price": 699, "warehouse": "WH1", "stock": 35, "tags": ["digital", "featured"]},
            {"_id": 104, "sku": "SKU-3001", "category": "service", "price": 299, "warehouse": "WH3", "stock": 999, "tags": ["subscription"]},
        ]
        orders = [
            {
                "_id": 10001,
                "orderNo": "ORD-2026-0001",
                "customerId": 1,
                "status": "PAID",
                "channel": "web",
                "amount": 216,
                "discount": 16,
                "items": [
                    {"sku": "SKU-1001", "qty": 2, "price": 88, "tags": ["paper"]},
                    {"sku": "SKU-1002", "qty": 1, "price": 128, "tags": ["paper", "bundle"]},
                ],
                "shipping": {"city": "Shanghai", "express": True},
                "note": "priority delivery",
                "createdAt": datetime.fromisoformat("2026-04-01T01:10:00+00:00"),
            },
            {
                "_id": 10002,
                "orderNo": "ORD-2026-0002",
                "customerId": 2,
                "status": "NEW",
                "channel": "store",
                "amount": 699,
                "discount": 0,
                "items": [{"sku": "SKU-2001", "qty": 1, "price": 699, "tags": ["digital", "featured"]}],
                "shipping": {"city": "Beijing", "express": False},
                "note": "offline pickup",
                "createdAt": datetime.fromisoformat("2026-04-01T03:20:00+00:00"),
            },
            {
                "_id": 10003,
                "orderNo": "ORD-2026-0003",
                "customerId": 3,
                "status": "SHIPPED",
                "channel": "web",
                "amount": 387,
                "discount": 12,
                "items": [
                    {"sku": "SKU-1001", "qty": 1, "price": 88, "tags": ["paper"]},
                    {"sku": "SKU-3001", "qty": 1, "price": 299, "tags": ["subscription"]},
                ],
                "shipping": {"city": "Shenzhen", "express": True},
                "note": "gift card attached",
                "createdAt": datetime.fromisoformat("2026-04-02T05:45:00+00:00"),
            },
            {
                "_id": 10004,
                "orderNo": "ORD-2026-0004",
                "customerId": 4,
                "status": "CANCELLED",
                "channel": "app",
                "amount": 128,
                "discount": 28,
                "items": [{"sku": "SKU-1002", "qty": 1, "price": 128, "tags": ["paper"]}],
                "shipping": {"city": "Hangzhou", "express": False},
                "note": "customer cancelled",
                "createdAt": datetime.fromisoformat("2026-04-03T07:00:00+00:00"),
            },
        ]

        database["customers"].insert_many(customers, ordered=True)
        database["inventory"].insert_many(inventory, ordered=True)
        database["orders"].insert_many(orders, ordered=True)
        _emit(progress_callback, "[TESTKIT] Inserted seed documents")

        created_indexes = {
            "orders": [
                database["orders"].create_index([("status", ASCENDING), ("createdAt", DESCENDING)], name="idx_status_createdAt"),
                database["orders"].create_index([("customerId", ASCENDING)], name="idx_customerId"),
            ],
            "inventory": [
                database["inventory"].create_index([("sku", ASCENDING)], unique=True, name="idx_sku"),
            ],
        }
        _emit(progress_callback, "[TESTKIT] Created seed indexes")

        return MongoSeedResult(
            database_name=database_name,
            finished_at=datetime.now().isoformat(timespec="seconds"),
            dropped_collections=dropped_collections,
            inserted_counts={
                "customers": len(customers),
                "inventory": len(inventory),
                "orders": len(orders),
            },
            created_indexes=created_indexes,
        )
    except PyMongoError as exc:
        raise RuntimeError(f"MongoDB 初始化测试数据失败: {exc}") from exc
    finally:
        if client is not None:
            client.close()


def run_profile_exercises(
    mongodb_uri: str,
    database_name: str,
    progress_callback: ProgressCallback = None,
) -> MongoExerciseResult:
    client = None
    original_profile_level = 0
    original_slowms = 100
    try:
        _emit(progress_callback, f"[TESTKIT] Connect exercise -> {database_name}")
        client = MongoClient(mongodb_uri, tz_aware=True, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        database = client[database_name]

        profile_info = database.command({"profile": -1})
        original_profile_level = int(profile_info.get("was", 0) or 0)
        original_slowms = int(profile_info.get("slowms", 100) or 100)
        profile_count_before = database["system.profile"].count_documents({}) if "system.profile" in database.list_collection_names() else 0
        _emit(progress_callback, f"[TESTKIT] Current profiler level={original_profile_level}, slowms={original_slowms}")

        database.command({"profile": 2, "slowms": 0})
        _emit(progress_callback, "[TESTKIT] Profiler set to level 2 with slowms=0")

        command_count = 0
        sample_suffix = datetime.now().strftime("%Y%m%d%H%M%S")

        def run_command(label: str, command: dict) -> None:
            nonlocal command_count
            _emit(progress_callback, f"[TESTKIT] Run {label}")
            database.command(command)
            command_count += 1

        run_command(
            "insert orders sample",
            {
                "insert": "orders",
                "documents": [
                    {
                        "_id": int(sample_suffix),
                        "orderNo": f"ORD-TEST-{sample_suffix}",
                        "customerId": 2,
                        "status": "NEW",
                        "channel": "web",
                        "amount": 159,
                        "discount": 9,
                        "tags": [],
                        "items": [
                            {"sku": "SKU-1001", "qty": 1, "price": 88, "tags": ["paper"]},
                            {"sku": "SKU-3001", "qty": 1, "price": 71, "tags": ["subscription"]},
                        ],
                        "shipping": {"city": "Nanjing", "express": True},
                        "note": "profile insert sample",
                        "createdAt": datetime.now(),
                    }
                ],
            },
        )
        run_command(
            "find supported operators",
            {
                "find": "orders",
                "filter": {
                    "status": {"$in": ["PAID", "SHIPPED"]},
                    "amount": {"$gte": 100, "$lt": 800, "$ne": 500},
                    "shipping.city": {"$regex": "^Sh", "$options": "i"},
                    "items": {"$elemMatch": {"qty": {"$gt": 0}, "tags": {"$all": ["paper"]}}},
                    "$and": [{"channel": {"$nin": ["partner"]}}, {"note": {"$exists": True}}],
                    "$or": [{"discount": {"$eq": 16}}, {"discount": {"$lte": 20}}],
                    "$nor": [{"status": "CANCELLED"}],
                    "channel": {"$not": {"$eq": "legacy"}},
                    "tags": {"$size": 0},
                },
                "projection": {"orderNo": 1, "status": 1, "amount": 1, "items": {"$slice": 1}},
            },
        )
        run_command(
            "find with $expr",
            {
                "find": "orders",
                "filter": {"$expr": {"$gt": ["$amount", "$discount"]}},
                "projection": {"orderNo": 1, "amount": 1, "discount": 1},
            },
        )
        run_command(
            "aggregate supported pipeline",
            {
                "aggregate": "orders",
                "pipeline": [
                    {"$match": {"status": {"$in": ["PAID", "SHIPPED", "NEW"]}, "amount": {"$gt": 50}}},
                    {
                        "$project": {
                            "orderNo": 1,
                            "customerId": 1,
                            "amount": 1,
                            "discount": 1,
                            "itemCount": {"$size": "$items"},
                            "finalAmount": {"$cond": [{"$gt": ["$discount", 0]}, {"$subtract": ["$amount", "$discount"]}, "$amount"]},
                            "emailText": {"$ifNull": ["$note", "missing"]},
                            "itemSkus": {"$map": {"input": "$items", "as": "item", "in": {"$concat": ["$$item.sku", "-", {"$toString": "$$item.qty"}]}}},
                            "paperItems": {"$filter": {"input": "$items", "as": "item", "cond": {"$in": ["paper", "$$item.tags"]}}},
                            "qtyTotal": {"$reduce": {"input": "$items", "initialValue": 0, "in": {"$add": ["$$value", "$$this.qty"]}}},
                            "orderDateText": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}},
                            "orderYear": {"$year": "$createdAt"},
                            "orderMonth": {"$month": "$createdAt"},
                            "orderDay": {"$dayOfMonth": "$createdAt"},
                        }
                    },
                    {
                        "$lookup": {
                            "from": "customers",
                            "localField": "customerId",
                            "foreignField": "_id",
                            "as": "customerInfo",
                            "pipeline": [{"$project": {"name": 1, "tier": 1, "tags": 1, "emailSafe": {"$ifNull": ["$email", "unknown"]}}}],
                        }
                    },
                    {"$unwind": "$customerInfo"},
                    {
                        "$facet": {
                            "byStatus": [
                                {"$group": {"_id": "$status", "orderCount": {"$sum": 1}, "amountAvg": {"$avg": "$amount"}, "amountMin": {"$min": "$amount"}, "amountMax": {"$max": "$amount"}}},
                                {"$sort": {"_id": 1}},
                            ],
                            "expensiveOrders": [
                                {"$match": {"amount": {"$gte": 200}}},
                                {"$count": "total"},
                            ],
                        }
                    },
                ],
                "cursor": {},
            },
        )
        run_command(
            "aggregate with $bucketAuto",
            {
                "aggregate": "orders",
                "pipeline": [{"$bucketAuto": {"groupBy": "$amount", "buckets": 3}}],
                "cursor": {},
            },
        )
        run_command(
            "aggregate with $graphLookup",
            {
                "aggregate": "customers",
                "pipeline": [
                    {"$graphLookup": {"from": "customers", "startWith": "$referrerCode", "connectFromField": "referrerCode", "connectToField": "customerCode", "as": "referralChain"}},
                    {"$project": {"customerCode": 1, "referralChain": 1}},
                ],
                "cursor": {},
            },
        )
        run_command(
            "aggregate with $regexMatch and $setField",
            {
                "aggregate": "orders",
                "pipeline": [
                    {
                        "$project": {
                            "orderNo": 1,
                            "noteHasPriority": {"$regexMatch": {"input": {"$ifNull": ["$note", ""]}, "regex": "priority", "options": "i"}},
                            "statusLabel": {"$setField": {"field": "label", "input": {"currentStatus": "$status"}, "value": "$status"}},
                        }
                    }
                ],
                "cursor": {},
            },
        )
        run_command(
            "aggregate set/unset/skip/limit",
            {
                "aggregate": "orders",
                "pipeline": [
                    {"$match": {"status": {"$ne": "CANCELLED"}}},
                    {"$set": {"amountBucket": {"$cond": [{"$gte": ["$amount", 200]}, "high", "normal"]}}},
                    {"$unset": "comment"},
                    {"$sort": {"createdAt": -1}},
                    {"$skip": 0},
                    {"$limit": 3},
                    {"$project": {"orderNo": 1, "amountBucket": 1, "createdAt": 1}},
                ],
                "cursor": {},
            },
        )
        run_command(
            "aggregate merge",
            {
                "aggregate": "orders",
                "pipeline": [
                    {"$match": {"status": {"$ne": "CANCELLED"}}},
                    {"$group": {"_id": "$status", "totalAmount": {"$sum": "$amount"}, "avgAmount": {"$avg": "$amount"}}},
                    {"$merge": {"into": "order_metrics", "on": "_id", "whenMatched": "replace", "whenNotMatched": "insert"}},
                ],
                "cursor": {},
            },
        )
        run_command(
            "aggregate out",
            {
                "aggregate": "orders",
                "pipeline": [
                    {"$match": {"status": {"$ne": "CANCELLED"}}},
                    {"$project": {"orderNo": 1, "status": 1, "amount": 1}},
                    {"$out": "order_archive"},
                ],
                "cursor": {},
            },
        )
        run_command(
            "update orders",
            {
                "update": "orders",
                "updates": [
                    {
                        "q": {"orderNo": "ORD-2026-0001", "amount": {"$gte": 200}},
                        "u": {
                            "$set": {"shipping.city": "Shanghai Pudong", "reviewedBy": "tester"},
                            "$unset": {"obsoleteField": ""},
                            "$inc": {"amount": 10},
                            "$push": {"tags": "manual-review"},
                            "$addToSet": {"tags": "gold-path"},
                            "$rename": {"note": "comment"},
                            "$min": {"discount": 8},
                            "$max": {"amount": 226},
                        },
                        "multi": False,
                    },
                    {
                        "q": {"status": {"$eq": "NEW"}},
                        "u": {"$pull": {"items": {"tags": {"$in": ["obsolete"]}}}, "$set": {"reviewedBy": "qa-bot"}},
                        "multi": True,
                    },
                ],
                "ordered": True,
            },
        )
        run_command(
            "delete cancelled orders",
            {
                "delete": "orders",
                "deletes": [{"q": {"status": "CANCELLED"}, "limit": 1}],
                "ordered": True,
            },
        )
        run_command(
            "findAndModify orders",
            {
                "findAndModify": "orders",
                "query": {"status": "PAID"},
                "update": {"$set": {"auditedAt": datetime.now(), "auditFlag": True}, "$inc": {"amount": 1}},
                "new": True,
            },
        )
        run_command("distinct orders status", {"distinct": "orders", "key": "status", "query": {"amount": {"$gt": 0}}})
        run_command("count orders", {"count": "orders", "query": {"status": {"$in": ["PAID", "SHIPPED", "NEW"]}}})
        run_command("listIndexes orders", {"listIndexes": "orders"})
        run_command(
            "createIndexes orders",
            {
                "createIndexes": "orders",
                "indexes": [{"key": {"channel": 1, "status": 1}, "name": "idx_channel_status_tmp"}],
            },
        )
        run_command("dropIndexes orders", {"dropIndexes": "orders", "index": "idx_channel_status_tmp"})

        profile_count_after = database["system.profile"].count_documents({})
        _emit(progress_callback, f"[TESTKIT] Completed exercise commands={command_count}, profile_count={profile_count_after}")

        return MongoExerciseResult(
            database_name=database_name,
            finished_at=datetime.now().isoformat(timespec="seconds"),
            command_count=command_count,
            original_profile_level=original_profile_level,
            restored_profile_level=original_profile_level,
            profile_count_before=profile_count_before,
            profile_count_after=profile_count_after,
            unsupported_feature_names=["$expr", "$bucketAuto", "$graphLookup", "$regexMatch", "$setField"],
        )
    except PyMongoError as exc:
        raise RuntimeError(f"MongoDB 执行测试查询失败: {exc}") from exc
    finally:
        if client is not None:
            try:
                database = client[database_name]
                database.command({"profile": original_profile_level, "slowms": original_slowms})
                _emit(progress_callback, f"[TESTKIT] Restored profiler level={original_profile_level}, slowms={original_slowms}")
            except Exception:
                pass
            client.close()
