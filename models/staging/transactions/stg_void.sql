SELECT 
  id,
  user_id,
  order_id,
  TIMESTAMP(created_at) AS created_at,
  kind,
  status,
  error_code,
  amount,
  currency,
  gateway,
  test,
  _sdc_shop_myshopify_domain,
  _sdc_shop_name,
  source_name,
  message,
  parent_id
FROM {{ ref('stg_transaction') }}
WHERE kind = "void"