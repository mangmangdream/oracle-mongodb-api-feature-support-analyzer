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

- 直接连接 MongoDB，数据库名从 URI 路径部分解析，不再单独输入。
- 读取 `<database>.system.profile`，支持时间窗口和采样上限。
- 兼容 `command`、`query`、`updateobj` 等常见 profile 结构，尽量还原旧版记录中的实际命令语义。
- 提取 `command`、`stage`、`operator`、`expression` 四类特征。
- 对未知命令事件和未映射到 Oracle 明细的特征单独计数，避免把“未识别”误判成“未使用”。
- 将观察到的 API 映射到 Oracle `Feature Support` 明细。
- 基于规则文件评估迁移必要性、迁移复杂度、推荐动作和热点项。
- 支持按 Oracle 目标版本和部署方式重算有效支持状态。
- 支持在页面中编辑并保存 `customer_overrides.csv`。

当前限制：

- 覆盖规则编辑器暂时只开放 `override_complexity` 和 `override_reason`，`override_scope` / `override_action` 仍需补齐前端入口。
- 热点项和排除项已参与导出，但 UI 里还没有完整独立视图。
- 采样覆盖度、置信度、结果对比等交付型能力尚未实现。

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

## 交付路线图

应用的核心目标不是只展示兼容性明细，而是帮助交付团队把 MongoDB 到 Oracle Database API for MongoDB 的迁移评估转成可执行工作项。

当前设计方向改为双轨演进：

- `usage-hardening`
  - 当前默认主线
  - 在现有 `system.profile` 分析模型上继续增强，优先提升解析覆盖率、未知项显式暴露、结论置信度和交付视图完整度
- `collector-lite`
  - 当前实验线
  - 引入轻量级多源证据采集，验证实例级、数据库级、集合级元数据是否能显著提升迁移判断质量

两条路线不作为长期并行产品维护。原则是先在实验线验证收益，再决定是否把 `collector-lite` 的有效能力并回主线。

### P0

主线 `usage-hardening`：

- 补全 profile 解析覆盖率，兼容更多 `system.profile` 结构
- 显式暴露未知命令、未映射特征、fallback 分类结果
- 补全覆盖规则编辑器，支持 `override_scope`、`override_action`
- 为热点项、排除项、未分类 API 提供独立表格视图
- 把分析结果整理为迁移工作包，如查询改写、聚合改写、语义验证、索引复核、阻塞项排查

实验线 `collector-lite`：

- 增加实例级 preflight，如 `ping`、`hello`、`buildInfo`、`connectionStatus`
- 增加基础结构采集，如 `listDatabases`、`dbStats`、`listCollections`、`collStats`、`listIndexes`
- 评估这些额外证据对迁移复杂度和优先级判断的增益

### P1

主线 `usage-hardening`：

- 增加采样覆盖度和结论置信度评分
- 标记需要补采样的低置信度 API
- 为复杂度调整、优先级排序、范围判断提供更完整的解释字段展示
- 支持更丰富的证据展示，而不是只保留单条样本

实验线 `collector-lite`：

- 引入分片和部署结构感知，验证 `shardingState`、`listShards`、`balancerStatus`、`getShardMap` 的价值
- 统一 workload 证据和结构证据的置信度表达
- 评估是否需要在 profiler 不充分时增加日志回退证据

### P2

路线收敛与程序化能力：

- 支持多次分析结果对比
- 支持不同环境和不同 Oracle 目标版本对比
- 支持跟踪规则调整和客户覆盖前后的评估变化
- 基于实验结果决定是否把 `collector-lite` 的部分能力合并进主线

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
