const dbName = "oracle_mongo_api_test";
const dbRef = db.getSiblingDB(dbName);

dbRef.setProfilingLevel(2, { slowms: 0 });

dbRef.runCommand({
  insert: "orders",
  documents: [
    {
      _id: 10005,
      orderNo: "ORD-2026-0005",
      customerId: 2,
      status: "NEW",
      channel: "web",
      amount: 159,
      discount: 9,
      tags: [],
      items: [
        { sku: "SKU-1001", qty: 1, price: 88, tags: ["paper"] },
        { sku: "SKU-3001", qty: 1, price: 71, tags: ["subscription"] },
      ],
      shipping: { city: "Nanjing", express: true },
      note: "profile insert sample",
      createdAt: new Date(),
    },
  ],
});

dbRef.runCommand({
  find: "orders",
  filter: {
    status: { $in: ["PAID", "SHIPPED"] },
    amount: { $gte: 100, $lt: 800, $ne: 500 },
    "shipping.city": { $regex: "^Sh", $options: "i" },
    items: {
      $elemMatch: {
        qty: { $gt: 0 },
        tags: { $all: ["paper"] },
      },
    },
    $and: [
      { channel: { $nin: ["partner"] } },
      { note: { $exists: true } },
    ],
    $or: [
      { discount: { $eq: 16 } },
      { discount: { $lte: 20 } },
    ],
    $nor: [
      { status: "CANCELLED" },
    ],
    channel: { $not: { $eq: "legacy" } },
    tags: { $size: 0 },
  },
  projection: {
    orderNo: 1,
    status: 1,
    amount: 1,
    items: { $slice: 1 },
  },
});

dbRef.runCommand({
  find: "orders",
  filter: {
    $expr: {
      $gt: ["$amount", "$discount"],
    },
  },
  projection: {
    orderNo: 1,
    amount: 1,
    discount: 1,
  },
});

dbRef.runCommand({
  aggregate: "orders",
  pipeline: [
    {
      $match: {
        status: { $in: ["PAID", "SHIPPED", "NEW"] },
        amount: { $gt: 50 },
      },
    },
    {
      $project: {
        orderNo: 1,
        customerId: 1,
        amount: 1,
        discount: 1,
        itemCount: { $size: "$items" },
        finalAmount: { $cond: [{ $gt: ["$discount", 0] }, { $subtract: ["$amount", "$discount"] }, "$amount"] },
        emailText: { $ifNull: ["$note", "missing"] },
        itemSkus: {
          $map: {
            input: "$items",
            as: "item",
            in: { $concat: ["$$item.sku", "-", { $toString: "$$item.qty" }] },
          },
        },
        paperItems: {
          $filter: {
            input: "$items",
            as: "item",
            cond: { $in: ["paper", "$$item.tags"] },
          },
        },
        qtyTotal: {
          $reduce: {
            input: "$items",
            initialValue: 0,
            in: { $add: ["$$value", "$$this.qty"] },
          },
        },
        orderDateText: { $dateToString: { format: "%Y-%m-%d", date: "$createdAt" } },
        orderYear: { $year: "$createdAt" },
        orderMonth: { $month: "$createdAt" },
        orderDay: { $dayOfMonth: "$createdAt" },
      },
    },
    {
      $lookup: {
        from: "customers",
        localField: "customerId",
        foreignField: "_id",
        as: "customerInfo",
        pipeline: [
          { $project: { name: 1, tier: 1, tags: 1, emailSafe: { $ifNull: ["$email", "unknown"] } } },
        ],
      },
    },
    { $unwind: "$customerInfo" },
    {
      $facet: {
        byStatus: [
          { $group: { _id: "$status", orderCount: { $sum: 1 }, amountAvg: { $avg: "$amount" }, amountMin: { $min: "$amount" }, amountMax: { $max: "$amount" } } },
          { $sort: { _id: 1 } },
        ],
        expensiveOrders: [
          { $match: { amount: { $gte: 200 } } },
          { $count: "total" },
        ],
      },
    },
  ],
  cursor: {},
});

dbRef.runCommand({
  aggregate: "orders",
  pipeline: [
    {
      $bucketAuto: {
        groupBy: "$amount",
        buckets: 3,
      },
    },
  ],
  cursor: {},
});

dbRef.runCommand({
  aggregate: "customers",
  pipeline: [
    {
      $graphLookup: {
        from: "customers",
        startWith: "$referrerCode",
        connectFromField: "referrerCode",
        connectToField: "customerCode",
        as: "referralChain",
      },
    },
    {
      $project: {
        customerCode: 1,
        referralChain: 1,
      },
    },
  ],
  cursor: {},
});

dbRef.runCommand({
  aggregate: "orders",
  pipeline: [
    {
      $project: {
        orderNo: 1,
        noteHasPriority: {
          $regexMatch: {
            input: { $ifNull: ["$note", ""] },
            regex: "priority",
            options: "i",
          },
        },
        statusLabel: {
          $setField: {
            field: "label",
            input: {
              currentStatus: "$status",
            },
            value: "$status",
          },
        },
      },
    },
  ],
  cursor: {},
});

dbRef.runCommand({
  aggregate: "orders",
  pipeline: [
    { $match: { status: { $ne: "CANCELLED" } } },
    { $set: { amountBucket: { $cond: [{ $gte: ["$amount", 200] }, "high", "normal"] } } },
    { $unset: "comment" },
    { $sort: { createdAt: -1 } },
    { $skip: 0 },
    { $limit: 3 },
    { $project: { orderNo: 1, amountBucket: 1, createdAt: 1 } },
  ],
  cursor: {},
});

dbRef.runCommand({
  aggregate: "orders",
  pipeline: [
    { $match: { status: { $ne: "CANCELLED" } } },
    { $group: { _id: "$status", totalAmount: { $sum: "$amount" }, avgAmount: { $avg: "$amount" } } },
    { $merge: { into: "order_metrics", on: "_id", whenMatched: "replace", whenNotMatched: "insert" } },
  ],
  cursor: {},
});

dbRef.runCommand({
  aggregate: "orders",
  pipeline: [
    { $match: { status: { $ne: "CANCELLED" } } },
    { $project: { orderNo: 1, status: 1, amount: 1 } },
    { $out: "order_archive" },
  ],
  cursor: {},
});

dbRef.runCommand({
  update: "orders",
  updates: [
    {
      q: {
        orderNo: "ORD-2026-0001",
        amount: { $gte: 200 },
      },
      u: {
        $set: { "shipping.city": "Shanghai Pudong", reviewedBy: "tester" },
        $unset: { obsoleteField: "" },
        $inc: { amount: 10 },
        $push: { tags: "manual-review" },
        $addToSet: { tags: "gold-path" },
        $rename: { note: "comment" },
        $min: { discount: 8 },
        $max: { amount: 226 },
      },
      multi: false,
    },
    {
      q: {
        status: { $eq: "NEW" },
      },
      u: {
        $pull: { items: { tags: { $in: ["obsolete"] } } },
        $set: { reviewedBy: "qa-bot" },
      },
      multi: true,
    },
  ],
  ordered: true,
});

dbRef.runCommand({
  delete: "orders",
  deletes: [
    {
      q: {
        status: "CANCELLED",
      },
      limit: 1,
    },
  ],
  ordered: true,
});

dbRef.runCommand({
  findAndModify: "orders",
  query: {
    status: "PAID",
  },
  update: {
    $set: {
      auditedAt: new Date(),
      auditFlag: true,
    },
    $inc: {
      amount: 1,
    },
  },
  new: true,
});

dbRef.runCommand({
  distinct: "orders",
  key: "status",
  query: {
    amount: { $gt: 0 },
  },
});

dbRef.runCommand({
  count: "orders",
  query: {
    status: { $in: ["PAID", "SHIPPED", "NEW"] },
  },
});

dbRef.runCommand({
  listIndexes: "orders",
});

dbRef.runCommand({
  createIndexes: "orders",
  indexes: [
    {
      key: { channel: 1, status: 1 },
      name: "idx_channel_status_tmp",
    },
  ],
});

dbRef.runCommand({
  dropIndexes: "orders",
  index: "idx_channel_status_tmp",
});

print("Profile exercise complete.");
printjson(dbRef.system.profile.find({}, { ts: 1, op: 1, ns: 1, command: 1, millis: 1 }).sort({ ts: -1 }).limit(5).toArray());
