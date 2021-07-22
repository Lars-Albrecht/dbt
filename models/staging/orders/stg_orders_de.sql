WITH ORDERS AS(
SELECT 
  /* deduplicate and get last entry */
  row_number() OVER (PARTITION BY o.id ORDER BY updated_at DESC) AS row_number,
  number,
  o.id AS shopify_transaction_id,
  created_at AS ordered_at_utc,
  total_price AS customer_payment,
  total_discounts,
  tl.value.price AS tax_amount,	
  SAFE_CAST(total_shipping_price_set.shop_money.amount AS FLOAT64) AS shipping_costs,
  ROUND(total_price + total_discounts - SAFE_CAST(total_shipping_price_set.shop_money.amount AS FLOAT64),2) AS subtotal,
  tl.value.rate AS tax_rate,
  tl.value.title As tax_title,
  currency,
  test,
  processing_method,
  gateway,
  sl.value.code AS shipping_method,
  shipping_address.country As shipping_country,
  tags,
  note,
  financial_status,
  fulfillment_status,
  f.value.created_at AS shopify_fulfilled_at,
  cancelled_at AS order_cancelled_at,
  updated_at AS order_updated_at,
  processed_at AS order_processed_at,
  closed_at AS order_closed_at,
  order_number,
  name AS shop_order_reference,
  user_id AS user_id,
  customer.id AS customer_id,
  o.checkout_id AS checkout_id,
FROM {{source('shopify_de', 'orders')}} o
LEFT JOIN UNNEST(fulfillments) AS f
LEFT JOIN UNNEST(tax_lines) AS tl
LEFT JOIN UNNEST(shipping_lines) AS sl
),
REFUNDS AS(
SELECT 
  transaction_id,	
  SUM(amount) AS sum_refund_amount,
  DATE(SPLIT(MAX(created_at),"T")[SAFE_OFFSET(0)]) AS last_refund_at
FROM {{ ref('stg_refunds_amount_per_order_de') }} 
GROUP BY transaction_id
)

SELECT * EXCEPT(row_number, transaction_id, line_item_id)
FROM ORDERS o
LEFT JOIN {{ref('stg_coupon_codes_de')}} cc ON  cc.transaction_id  = o.shopify_transaction_id
LEFT JOIN {{ref('stg_freegift_codes_de')}} sc ON  sc.transaction_id = o.shopify_transaction_id
LEFT JOIN REFUNDS r ON r.transaction_id = o.shopify_transaction_id
WHERE row_number = 1
AND test = false
ORDER BY number ASC