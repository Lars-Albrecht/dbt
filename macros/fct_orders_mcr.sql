{% macro insert_order_query_with_param(country) %}

WITH ORDERS AS(
SELECT 
  /* deduplicate and get last entry */
  row_number() OVER (PARTITION BY o.id ORDER BY updated_at DESC) AS row_number,
  number,
  o.id AS shopify_transaction_id,
  TO_BASE64(MD5(UPPER(email))) AS email_hash,
  created_at AS ordered_at_utc,
  total_price AS customer_payment,
  total_discounts,

  SAFE_CAST(total_shipping_price_set.shop_money.amount AS FLOAT64) AS shipping_costs,
  ROUND(total_price + total_discounts - SAFE_CAST(total_shipping_price_set.shop_money.amount AS FLOAT64),2) AS subtotal,

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
  UPPER(dc.value.code) AS code,
  order_number,
  source_name,
  name AS shop_order_reference,
  user_id AS user_id,
  customer.id AS customer_id,
  o.checkout_id AS checkout_id,
FROM {{source(['shopify','_',country]|join, 'orders')}} o
LEFT JOIN UNNEST(fulfillments) AS f

LEFT JOIN UNNEST(shipping_lines) AS sl
LEFT JOIN UNNEST(discount_codes) AS dc
),
REFUNDS AS(
SELECT 
  transaction_id,	
  SUM(amount) AS sum_refund_amount,
  DATE(SPLIT(MAX(created_at),"T")[SAFE_OFFSET(0)]) AS last_refund_at
FROM {{ ref(['stg_refunds_amount_per_order_',country]|join) }} 
GROUP BY transaction_id
), TAXES AS(
SELECT 
  o.id AS transaction_id,		
  SUM(tl.value.price) AS tax_amount,	
  AVG(tl.value.rate) AS tax_rate,
  tl.value.title As tax_title,
FROM `leslunes-raw`.`shopify_de`.`orders` o
LEFT JOIN UNNEST(tax_lines) AS tl
GROUP BY 1,4
)


SELECT * EXCEPT(row_number, transaction_id, source_name, created_at, email_hash),
  o.email_hash,
  CASE WHEN o.ordered_at_utc = c.first_purchase_date THEN 1 ELSE 0 END AS new_customer,
  CASE WHEN o.ordered_at_utc = c.first_purchase_date THEN 0 ELSE 1 END AS returning_customer,
  CASE WHEN source_name = "580111" THEN "web" ELSE source_name END AS source_name
FROM ORDERS o
LEFT JOIN REFUNDS r ON r.transaction_id = o.shopify_transaction_id
LEFT JOIN TAXES t ON t.transaction_id = o.shopify_transaction_id
LEFT JOIN  {{ ref(['stg_first_purchase_',country]|join)}} c ON c.email_hash = o.email_hash and o.ordered_at_utc = c.created_at
WHERE row_number = 1
AND test = false
ORDER BY number ASC

{% endmacro %}