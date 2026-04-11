const dbName = "oracle_mongo_api_test";
const dbRef = db.getSiblingDB(dbName);

dbRef.setProfilingLevel(0);
print(`Profiling disabled for ${dbName}`);
