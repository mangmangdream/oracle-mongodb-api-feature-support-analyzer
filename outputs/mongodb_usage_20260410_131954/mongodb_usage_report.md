# MongoDB Usage Analysis

- 数据库: oracle_mongo_api_test
- 抓取时间: 2026-04-10T13:19:53
- 时间范围开始: 未设置
- 时间范围结束: 未设置
- profile 记录数: 22
- 功能项数: 66
- 结果是否截断: 否

## 支持状态汇总

| metric                | label         |   count |   usage_count |   percentage |
|:----------------------|:--------------|--------:|--------------:|-------------:|
| oracle_support_status | Not Supported |       2 |             2 |         3.03 |
| oracle_support_status | Supported     |      64 |            86 |        96.97 |

## 重点关注项

| feature_type   | feature_name   | command_name   | op_type   | database              | collection   |   usage_count | oracle_support_status   | oracle_support_since   | oracle_category   |
|:---------------|:---------------|:---------------|:----------|:----------------------|:-------------|--------------:|:------------------------|:-----------------------|:------------------|
| command        | createIndexes  | createIndexes  | command   | oracle_mongo_api_test | orders       |             1 | Not Supported           | 26ai. No-op (19c)      | Database Commands |
| command        | dropIndexes    | dropIndexes    | command   | oracle_mongo_api_test | orders       |             1 | Not Supported           | 26ai. No-op (19c)      | Database Commands |
