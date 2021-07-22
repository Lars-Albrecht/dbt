SELECT  PARSE_DATE("%Y%m%d", DATE) AS date
       ,SAFE_CAST(sessions AS INT64) AS sessions
       ,SAFE_CAST(users AS INT64) AS users
       ,SAFE_CAST(sessions_with_product_views AS INT64) AS sessions_with_product_views
       ,SAFE_CAST(sessions_with_add_to_cart AS INT64) AS sessions_with_add_to_cart
       ,SAFE_CAST(sessions_with_checkout AS INT64) AS sessions_with_checkout
       ,SAFE_CAST(sessions_with_transactions AS INT64) AS sessions_with_transactions
FROM {{source('ga_kpi_metrics', 'report_fr')}}