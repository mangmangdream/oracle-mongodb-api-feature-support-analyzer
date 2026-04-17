# Oracle Database API for MongoDB 兼容性分析工具

这是一个基于 Streamlit 的分析应用，用来把 Oracle 官方 `Feature Support` 文档、MongoDB 实际运行画像和迁移复杂度评估放到同一个工作台里。

当前应用包含 3 个主能力：

- `文档同步`：抓取 Oracle 官方 `Feature Support` 明细，并可选同步 MongoDB 官方说明补充描述。
- `MongoDB Usage 分析`：连接目标 MongoDB，读取 `system.profile`，抽取实际使用到的 command、stage、operator、expression，并映射到 Oracle 支持矩阵。
- `MongoDB 测试工具`：为测试库初始化样本数据、执行预置查询、生成可被当前解析器识别的 `system.profile` 样本。

## 快速开始

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

默认访问地址：

- [http://localhost:8501](http://localhost:8501)

## 推荐使用流程

1. 在 `文档同步` 页签中确认 Oracle 文档链接。
2. 如需补充 MongoDB 官方说明，先点击 `同步 MongoDB 官方说明`。
3. 点击 `同步 Oracle 官方文档`，生成当前 Oracle `Feature Support` 基线。
4. 在 `MongoDB Usage 分析` 页签中填入包含默认数据库的 MongoDB URI，例如 `mongodb://user:password@host:27017/oracle_mongo_api_test?authSource=admin`。
5. 先执行 `测试连接`，确认目标库存在 `system.profile`。
6. 点击 `分析 system.profile`，查看实际使用 API、Oracle 支持状态、迁移复杂度、热点项和证据样本。
7. 如需快速准备测试样本，可使用 `MongoDB 测试工具` 页签初始化数据并执行预置查询。

## 当前功能边界

### 文档同步

- 抓取 Oracle 官方 `Feature Support` 表格并归一化支持状态。
- 从 Oracle 文档首页提取文档编号、版本时间，并与上次缓存比较。
- 可同步 MongoDB 官方参考说明，并补充到 Oracle 明细中。
- 支持缓存加载、离线 HTML 报告和 Excel 导出。

### Usage 分析

- 直接连接 MongoDB，支持可选 `Database` 输入；未指定时会枚举全部非系统库。
- workload 采集按 `system.profile -> global log -> serverStatus.metrics` 的优先级逐次尝试。
- 一旦命中前一个可用 workload 源，就不会继续收集后一个。
- `system.profile` 和 `global log` 作为 database-level 证据；`serverStatus.metrics` 仅作为实例级兜底证据。
- 支持时间窗口和采样上限。
- 提取 `command`、`stage`、`operator`、`expression` 四类特征。
- 将观察到的 API 映射到 Oracle `Feature Support` 明细。
- 基于规则文件评估迁移必要性、迁移复杂度、推荐动作和热点项。
- 支持按 Oracle 目标版本和部署方式重算有效支持状态。
- 支持在页面中编辑并保存 `customer_overrides.csv`。

`collector-lite` 实验方向补充：

- 采集方式应设计成显式用户选择，而不是在 `system.profile` 不可用时静默切换数据源。
- 推荐支持 `PROFILE_ONLY`、`AUTO`、`LOG_ONLY`、`METRICS_ONLY` 四类策略。
- 默认建议仍然是 `PROFILE_ONLY`。
- `AUTO` 应采用短路策略：`system.profile -> global log -> serverStatus.metrics`，命中首个可用 workload 源后停止。
- 如果最终落到 `serverStatus.metrics`，结果必须明确标记为实例级证据，而不是伪装成某个 database 的精确 workload。
- 每次分析结果都应暴露 `requested_strategy`、`resolved_strategy`、`effective_source`、`fallback_chain` 和 `confidence_level`。

### MongoDB 测试工具

- 测试连接并检测 `system.profile`。
- 重建测试集合：`customers`、`orders`、`inventory`、`order_metrics`、`order_archive`。
- 执行覆盖常见 command、stage、operator、expression 的预置查询。
- 临时提升 profiler 级别并在执行完成后恢复。

## 输出文件

### Oracle 文档同步

每次保存会写入 `outputs/feature_support_<timestamp>/`，主要包含：

- `feature_support_detail.csv`
- `feature_support_summary.csv`
- `document_metadata.json`
- `feature_support_report.html`
- `feature_support_analysis.xlsx`

MongoDB 说明缓存位于：

- `outputs/mongodb_reference_catalog.csv`
- `outputs/mongodb_reference_metadata.json`

### MongoDB Usage 分析

每次保存会写入 `outputs/mongodb_usage_<timestamp>/`，主要包含：

- `mongodb_usage_feature_detail.csv`
- `mongodb_usage_feature_summary.csv`
- `mongodb_usage_metadata.json`
- `mongodb_migration_complexity_detail.csv`
- `mongodb_migration_summary.csv`
- `mongodb_migration_hotspots.csv`
- `mongodb_migration_excluded_commands.csv`
- `mongodb_usage_report.html`
- `mongodb_usage_analysis.xlsx`

## 代码结构

- [app.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/app.py)：Streamlit UI、缓存管理、导出和页面编排
- [src/oracle_feature_support/fetcher.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/fetcher.py)：Oracle 文档抓取、表格解析、状态归一化、文档元数据比较
- [src/oracle_feature_support/mongodb_reference.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/mongodb_reference.py)：MongoDB 官方说明同步与补充
- [src/oracle_feature_support/mongodb_profile_reader.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/mongodb_profile_reader.py)：MongoDB 连接测试与 `system.profile` 读取
- [src/oracle_feature_support/profile_parser.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/profile_parser.py)：profile 标准化、特征提取、事件明细整理
- [src/oracle_feature_support/feature_mapper.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/feature_mapper.py)：MongoDB 特征到 Oracle 支持明细的映射
- [src/oracle_feature_support/migration_rules.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/migration_rules.py)：迁移规则和客户覆写加载、校验、保存
- [src/oracle_feature_support/migration_assessment.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/migration_assessment.py)：迁移复杂度、必要性、热点项和排除项评估
- [src/oracle_feature_support/usage_report.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/usage_report.py)：Usage 汇总和分析结果导出
- [src/oracle_feature_support/mongodb_testkit.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/mongodb_testkit.py)：测试数据初始化和预置查询执行
