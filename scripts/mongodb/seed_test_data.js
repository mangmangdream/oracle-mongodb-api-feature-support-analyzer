const dbName = "oracle_mongo_api_test";
const dbRef = db.getSiblingDB(dbName);

dbRef.customers.drop();
dbRef.orders.drop();
dbRef.inventory.drop();
dbRef.order_metrics.drop();
dbRef.order_archive.drop();

dbRef.customers.insertMany([
  {
    _id: 1,
    customerCode: "C001",
    referrerCode: null,
    name: "Alice Chen",
    tier: "gold",
    region: "SH",
    tags: ["vip", "b2b"],
    email: "alice.chen@example.com",
    createdAt: ISODate("2026-03-01T09:00:00Z"),
  },
  {
    _id: 2,
    customerCode: "C002",
    referrerCode: "C001",
    name: "Bob Wang",
    tier: "silver",
    region: "BJ",
    tags: ["retail"],
    email: "bob.wang@example.com",
    createdAt: ISODate("2026-03-03T10:15:00Z"),
  },
  {
    _id: 3,
    customerCode: "C003",
    referrerCode: "C001",
    name: "Cathy Liu",
    tier: "gold",
    region: "SZ",
    tags: ["vip", "online"],
    email: null,
    createdAt: ISODate("2026-03-05T11:30:00Z"),
  },
  {
    _id: 4,
    customerCode: "C004",
    referrerCode: "C003",
    name: "David Sun",
    tier: "bronze",
    region: "HZ",
    tags: ["online"],
    createdAt: ISODate("2026-03-08T08:20:00Z"),
  },
]);

dbRef.inventory.insertMany([
  { _id: 101, sku: "SKU-1001", category: "book", price: 88, warehouse: "WH1", stock: 120, tags: ["paper", "sale"] },
  { _id: 102, sku: "SKU-1002", category: "book", price: 128, warehouse: "WH2", stock: 80, tags: ["paper"] },
  { _id: 103, sku: "SKU-2001", category: "device", price: 699, warehouse: "WH1", stock: 35, tags: ["digital", "featured"] },
  { _id: 104, sku: "SKU-3001", category: "service", price: 299, warehouse: "WH3", stock: 999, tags: ["subscription"] },
]);

dbRef.orders.insertMany([
  {
    _id: 10001,
    orderNo: "ORD-2026-0001",
    customerId: 1,
    status: "PAID",
    channel: "web",
    amount: 216,
    discount: 16,
    items: [
      { sku: "SKU-1001", qty: 2, price: 88, tags: ["paper"] },
      { sku: "SKU-1002", qty: 1, price: 128, tags: ["paper", "bundle"] },
    ],
    shipping: { city: "Shanghai", express: true },
    note: "priority delivery",
    createdAt: ISODate("2026-04-01T01:10:00Z"),
  },
  {
    _id: 10002,
    orderNo: "ORD-2026-0002",
    customerId: 2,
    status: "NEW",
    channel: "store",
    amount: 699,
    discount: 0,
    items: [
      { sku: "SKU-2001", qty: 1, price: 699, tags: ["digital", "featured"] },
    ],
    shipping: { city: "Beijing", express: false },
    note: "offline pickup",
    createdAt: ISODate("2026-04-01T03:20:00Z"),
  },
  {
    _id: 10003,
    orderNo: "ORD-2026-0003",
    customerId: 3,
    status: "SHIPPED",
    channel: "web",
    amount: 387,
    discount: 12,
    items: [
      { sku: "SKU-1001", qty: 1, price: 88, tags: ["paper"] },
      { sku: "SKU-3001", qty: 1, price: 299, tags: ["subscription"] },
    ],
    shipping: { city: "Shenzhen", express: true },
    note: "gift card attached",
    createdAt: ISODate("2026-04-02T05:45:00Z"),
  },
  {
    _id: 10004,
    orderNo: "ORD-2026-0004",
    customerId: 4,
    status: "CANCELLED",
    channel: "app",
    amount: 128,
    discount: 28,
    items: [
      { sku: "SKU-1002", qty: 1, price: 128, tags: ["paper"] },
    ],
    shipping: { city: "Hangzhou", express: false },
    note: "customer cancelled",
    createdAt: ISODate("2026-04-03T07:00:00Z"),
  },
]);

dbRef.orders.createIndex({ status: 1, createdAt: -1 }, { name: "idx_status_createdAt" });
dbRef.orders.createIndex({ customerId: 1 }, { name: "idx_customerId" });
dbRef.inventory.createIndex({ sku: 1 }, { unique: true, name: "idx_sku" });

print(`Seeded database: ${dbName}`);
print(`customers=${dbRef.customers.countDocuments()}`);
print(`orders=${dbRef.orders.countDocuments()}`);
print(`inventory=${dbRef.inventory.countDocuments()}`);
