# MongoDB Usage Analysis

- 数据库: oracle_mongo_api_test
- 抓取时间: 2026-04-10T12:51:04
- 时间范围开始: 未设置
- 时间范围结束: 未设置
- profile 记录数: 18
- 功能项数: 62
- 结果是否截断: 否

## 支持状态汇总

| metric                | label         |   count |   usage_count |   percentage |
|:----------------------|:--------------|--------:|--------------:|-------------:|
| oracle_support_status | Not Supported |       2 |             2 |         3.23 |
| oracle_support_status | Supported     |      60 |            82 |        96.77 |

## 重点关注项

| feature_type   | feature_name   | command_name   | database              | collection   |   usage_count | oracle_support_status   | oracle_support_since   | oracle_category   |
|:---------------|:---------------|:---------------|:----------------------|:-------------|--------------:|:------------------------|:-----------------------|:------------------|
| command        | createIndexes  | createIndexes  | oracle_mongo_api_test | orders       |             1 | Not Supported           | 26ai. No-op (19c)      | Database Commands |
| command        | dropIndexes    | dropIndexes    | oracle_mongo_api_test | orders       |             1 | Not Supported           | 26ai. No-op (19c)      | Database Commands |
