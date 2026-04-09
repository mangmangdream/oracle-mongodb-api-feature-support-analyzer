# Oracle MongoDB API Feature Support 分析项目

通过手工点击触发，抓取并分析 Oracle MongoDB API 文档中的 `Feature Support` 明细。

目标链接（默认）：

- https://docs.oracle.com/en/database/oracle/mongodb-api/mgapi/support-mongodb-apis-operations-and-data-types-reference.html

## 功能

- 手工点击按钮后再执行抓取（非自动轮询）
- 提取 `Feature Support` 相关表格明细
- 自动归一化支持状态（`Supported` / `Partially Supported` / `Not Supported` / `Other`）
- 生成统计汇总（数量与占比）
- 导出明细和统计 CSV，并生成 `report.md`
- 从文档首页提取文档编号和版本时间，用于辅助判断是否有更新内容
- 可手工同步 MongoDB 官方文档中的 `command`、`operator`、`stage` 说明，并补充到明细表
- 可选显示执行日志（抓取重试、解析步骤、保存路径）用于排查问题

## 快速开始

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## 如何启动应用

如果已经安装过依赖，进入项目目录后直接启动：

```bash
cd /Users/qizou/aiworkspace/mongodbapi-feature-update
streamlit run app.py
```

如果是第一次运行，先安装依赖再启动：

```bash
cd /Users/qizou/aiworkspace/mongodbapi-feature-update
pip install -r requirements.txt
streamlit run app.py
```

启动成功后，浏览器会自动打开页面；如果没有自动打开，可以手动访问：

- http://localhost:8501

浏览器打开后：

1. 保持默认链接或替换目标链接
2. 如需更新 MongoDB 官方说明，手动点击 `同步 MongoDB 官方说明`
3. 点击 `开始抓取并分析`
4. 页面展示明细、汇总和补充说明，并自动保存结果到 `outputs/feature_support_时间戳/`

注意：

- `开始抓取并分析` 只会使用本地已缓存的 MongoDB 官方说明，不会自动联网更新说明内容
- 只有点击 `同步 MongoDB 官方说明` 才会从 MongoDB 在线文档抓取并更新说明缓存

## 输出文件

每次执行会在 `outputs/` 下生成新目录，例如：

- `feature_support_detail.csv`
- `feature_support_summary.csv`
- `document_metadata.json`
- `report.md`

MongoDB 官方说明缓存文件：

- `outputs/mongodb_reference_catalog.csv`
- `outputs/mongodb_reference_metadata.json`

## 代码结构

- `app.py`：Streamlit 页面与手工触发入口
- `src/oracle_feature_support/fetcher.py`：抓取、表格解析、状态归一化、统计与导出
