# Oracle Linux 8 MongoDB 测试环境准备

本文档用于在 Oracle Linux 8 上安装一套测试用 MongoDB Community，并准备可被当前项目识别的 `system.profile` 样本。

目标目录固定为 `/u01/app/mongodb`。

## 前提

- 操作系统: Oracle Linux 8
- 内核: RHCK
- 权限: `root`
- 网络: 能访问 MongoDB 官方 YUM 源

MongoDB 官方当前文档确认 Oracle Linux 8 支持 Community 7.0 和 8.0，且仅支持 RHCK，不支持 UEK：

- [MongoDB Platform Support](https://www.mongodb.com/docs/v8.0/installation/)
- [Install MongoDB Community Edition on Red Hat or CentOS](https://www.mongodb.com/docs/v8.0/tutorial/install-mongodb-on-red-hat/)

## 目录规划

在 `/u01/app/mongodb` 下使用以下结构：

- `conf/`
- `data/`
- `log/`
- `run/`
- `scripts/`

说明：

- `mongodb-org` RPM 安装的二进制仍位于系统标准路径，例如 `/usr/bin/mongod`
- 指定目录主要承载配置、数据、日志、运行时文件和测试脚本

## 安装

将仓库中的 [install_oracle_linux8_mongodb.sh](/Users/qizou/aiworkspace/mongodbapi-feature-update/scripts/mongodb/install_oracle_linux8_mongodb.sh) 拷贝到 Oracle Linux 8 主机并以 `root` 执行：

```bash
chmod +x scripts/mongodb/install_oracle_linux8_mongodb.sh
sudo ./scripts/mongodb/install_oracle_linux8_mongodb.sh /u01/app/mongodb
```

脚本会完成以下动作：

- 写入 MongoDB 8.0 YUM 源
- 安装 `mongodb-org`
- 创建 `/u01/app/mongodb` 下的配置、数据、日志目录
- 生成 `mongod-test` systemd 服务
- 将 `dbPath`、日志、PID 都指向 `/u01/app/mongodb`
- 尝试设置 SELinux 上下文

安装完成后验证：

```bash
mongosh --host 127.0.0.1 --port 27017 --eval 'db.adminCommand({ ping: 1 })'
systemctl status mongod-test --no-pager
```

## 造数

先导入基础数据：

```bash
mongosh --host 127.0.0.1 --port 27017 /Users/qizou/aiworkspace/mongodbapi-feature-update/scripts/mongodb/seed_test_data.js
```

该脚本会创建测试库 `oracle_mongo_api_test`，以及以下集合：

- `customers`
- `orders`
- `inventory`
- `order_metrics`
- `order_archive`

并建立这些索引：

- `orders.idx_status_createdAt`
- `orders.idx_customerId`
- `inventory.idx_sku`

## 生成 system.profile 样本

执行测试查询脚本：

```bash
mongosh --host 127.0.0.1 --port 27017 /Users/qizou/aiworkspace/mongodbapi-feature-update/scripts/mongodb/exercise_profile_queries.js
```

该脚本会：

- 将 `oracle_mongo_api_test` 的 profiler 调整为 `level 2`
- 执行 `find`
- 执行包含 `$expr` 的 `find`
- 执行 `aggregate`
- 执行包含 `$bucketAuto`、`$graphLookup`、`$regexMatch`、`$setField` 的 `aggregate`
- 执行 `update`
- 执行 `delete`
- 执行 `findAndModify`
- 执行 `distinct`
- 执行 `count`
- 执行 `listIndexes`
- 执行 `createIndexes`
- 执行 `dropIndexes`

并覆盖当前项目解析逻辑中这些能力：

- command: `find` `aggregate` `insert` `update` `delete` `findAndModify` `distinct` `count` `createIndexes` `dropIndexes` `listIndexes`
- stage: `$match` `$project` `$group` `$sort` `$skip` `$limit` `$count` `$lookup` `$facet` `$unwind` `$set` `$unset` `$merge` `$out` `$bucketAuto` `$graphLookup`
- operator: `$expr` `$in` `$nin` `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$exists` `$regex` `$and` `$or` `$not` `$nor` `$elemMatch` `$size` `$all` `$set` `$unset` `$inc` `$push` `$pull` `$addToSet` `$rename` `$min` `$max`
- expression: `$sum` `$avg` `$min` `$max` `$cond` `$ifNull` `$map` `$filter` `$reduce` `$concat` `$toString` `$dateToString` `$year` `$month` `$dayOfMonth` `$regexMatch` `$setField`

其中下面这些功能在 Oracle 官方支持表中是明确 `Not Supported`，适合用来做对比样本：

- `$expr`
- `$bucketAuto`
- `$graphLookup`
- `$regexMatch`
- `$setField`

## 查看 profiling 结果

```bash
mongosh --host 127.0.0.1 --port 27017 --eval '
const d = db.getSiblingDB("oracle_mongo_api_test");
printjson(d.system.profile.find({}, { ts: 1, op: 1, ns: 1, command: 1, millis: 1 }).sort({ ts: -1 }).limit(10).toArray());
'
```

也可以直接看 profile 总量：

```bash
mongosh --host 127.0.0.1 --port 27017 --eval '
const d = db.getSiblingDB("oracle_mongo_api_test");
print("profile count=" + d.system.profile.countDocuments());
'
```

## 在本项目中测试

当前应用有两个相关页签：

- `MongoDB 测试工具`
- `MongoDB Usage 分析`

推荐顺序如下。

### 方式一：直接用应用内测试工具造数

在 `MongoDB 测试工具` 中填入：

- MongoDB URI: `mongodb://127.0.0.1:27017/oracle_mongo_api_test`

然后依次执行：

1. `测试连接`
2. `初始化测试数据`
3. `运行测试查询`

注意：

- 该页会写入并覆盖测试集合
- 执行测试查询时会临时把 profiler 调整到 `level 2`
- 页面会在完成后恢复 profiler 原始级别

### 方式二：用脚本造数后执行 Usage 分析

在 `MongoDB Usage 分析` 中填入：

- MongoDB URI: `mongodb://127.0.0.1:27017/oracle_mongo_api_test`
- Start Time: 可留空，或填造数后时间窗口
- End Time: 可留空
- Max Sample Limit: `5000`

当前 UI 不再单独输入 `Database Name`，数据库名直接从 URI 路径部分解析。

点击 `分析 system.profile` 后，项目应能读到 `system.profile`，并输出：

- 实际观察到的 MongoDB API
- Oracle 支持状态映射
- 迁移必要性与迁移复杂度
- 热点项与证据样本

## 清理或关闭 profiler

如不再需要继续记录 profile：

```bash
mongosh --host 127.0.0.1 --port 27017 /Users/qizou/aiworkspace/mongodbapi-feature-update/scripts/mongodb/disable_profiling.js
```

如果要重置测试库：

```bash
mongosh --host 127.0.0.1 --port 27017 --eval 'db.getSiblingDB("oracle_mongo_api_test").dropDatabase()'
```
