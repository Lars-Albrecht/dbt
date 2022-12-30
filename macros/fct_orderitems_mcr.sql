{% macro insert_query_with_param(country) %}

WITH line_items_sets AS(
  SELECT row_number() OVER (PARTITION BY li.value.id, p.value.name ORDER BY updated_at DESC) AS row_number, 
  id,
  o.created_at,
  o.updated_at,
  o.note,
  o.number,
  o.order_number,
  o.name, 
  li.value.id AS line_item_id,
  li.value.title AS order_item, 
  li.value.quantity AS qty,
  li.value.variant_id AS variant_id,
  "set_item" AS item_type,
  email, 
  tags,
  p.value.name AS item_title,	
  p.value.value AS item_desc, 
   
   CASE WHEN REGEXP_CONTAINS(p.value.name, r"Set") THEN li.value.price_set.shop_money.amount ELSE "0" END AS amount,
  source_name
FROM  leslunes-raw.shopify_{{country}}.`orders` o
LEFT JOIN UNNEST(line_items) AS li
LEFT JOIN unnest(li.value.properties) p
WHERE test = False
),

line_items AS(
  SELECT 
    row_number() OVER (PARTITION BY li.value.id, li.value.sku ORDER BY updated_at DESC) AS row_number, 
    o.id, 
    li.value.sku, 
    note, 
    name, 
    created_at, 
    order_number,  
    li.value.title AS order_item,
    li.value.title AS item_title, 
    li.value.id AS line_item_id,
    li.value.price_set.shop_money.amount,
    li.value.variant_id AS variant_id,
    li.value.quantity as qty,
    CASE 
      WHEN 
        LOWER(li.value.title) LIKE '%set%' 
        OR LOWER(li.value.title) LIKE '%duo%' 
        OR LOWER(li.value.title) LIKE '%trio%' 
        OR LOWER(li.value.title) LIKE '%bundle%' 
        OR LOWER(li.value.title) LIKE '%team%'
       THEN 'set'
       ELSE "single_item" END AS item_type,
    email,
    tags,
    source_name
FROM leslunes-raw.shopify_{{country}}.orders o,
UNNEST(line_items) AS li
WHERE test = False
), tax AS (
  SELECT DISTINCT 
    id AS shopify_transaction_id ,tx.value.rate AS tax_rate, 
  FROM leslunes-raw.shopify_{{country}}.orders,
    UNNEST(line_items) AS li,
    UNNEST(li.value.tax_lines) AS tx
 
), shipping AS(
  SELECT DISTINCT
    o.id AS shopify_transaction_id,
    sl.value.code AS shipping_method,
    shipping_address.country As shipping_country
FROM  
    leslunes-raw.shopify_{{country}}.orders o,
    UNNEST(shipping_lines) AS sl
),
coupon_codes AS(
  SELECT distinct
    o.id AS shopify_transaction_id,
    dapp.value.value_type AS code_value_type,
    dapp.value.type AS code_type,	
    dapp.value.value AS code_value,
    dapp.value.code AS code
  FROM  
    leslunes-raw.shopify_{{country}}.orders o,
    UNNEST(o.discount_applications) AS dapp 
  WHERE dapp.value.type != "script"
),

discounts AS(
SELECT DISTINCT
  id AS transaction_id
  , li.value.id AS lineitem_id 
  , li.value.product_id 
  , li.value.price_set.shop_money.amount AS item_price
  , li.value.quantity
  , li.value.total_discount AS pre_cart_discount
  , liv.value.rate	AS tax_rate 
  , liv.value.price AS item_tax
  
  , ROUND( ((SAFE_CAST(li.value.price_set.shop_money.amount AS NUMERIC) * li.value.quantity ) -li.value.total_discount) * ((100-dapp.value.value)/100),2) AS discounted_item_price
  
  , ABS( li.value.total_discount -dall.value.amount) AS item_coupon_discount

  , CASE WHEN li.value.total_discount  = SAFE_CAST(li.value.price_set.shop_money.amount AS NUMERIC) * li.value.quantity  THEN TRUE ELSE FALSE END AS is_freegift
  
  , dapp.value.value AS code_value
  , dapp.value.code AS coupon_code
  , dapp.value.value_type AS code_type

  
FROM `leslunes-raw.shopify_{{country}}.orders` 
LEFT JOIN UNNEST(line_items) li
LEFT JOIN UNNEST(li.value.tax_lines) liv
LEFT JOIN UNNEST(discount_applications) dapp ON dapp.value.type = "discount_code"
LEFT JOIN UNNEST(li.value.discount_allocations) dall ON dall.value.amount > 0
),

final AS(
  SELECT 
    id AS shopify_transaction_id,
    email,
    created_at,
   
    note AS order_note, 
    name AS shop_order_ref,
    order_number, 
   
    order_item, 
    qty,
    variant_id,
    item_title,	
    item_desc, 
   
   CASE WHEN REGEXP_CONTAINS(item_title, r"Set") THEN amount ELSE "0" END AS amount,
   
   line_item_id AS LID,
   item_type,
   tags,
   source_name
FROM line_items_sets i

WHERE 
  row_number = 1
  AND item_title not in("ll_fg", "ll_hash","ll_min_total", "_ll_coupon_info")
  
UNION ALL

SELECT 
  id AS shopify_transaction_id, 
  email ,
  created_at,
 
  note AS order_note, 
  name AS shop_order_ref,
  order_number, 
 
  CASE WHEN sku = "" THEN order_item ELSE sku END AS order_item, 
  qty,
  variant_id,
  order_item AS item_title,
  sku AS item_desc,
 
  amount,
 
  line_item_id as LID,
  item_type, 
  tags,
  source_name
FROM line_items li 
WHERE 
  row_number = 1
  AND order_item not in("ll_fg", "ll_hash","ll_min_total", "_ll_coupon_info")
), first_purchase_date AS(

SELECT DISTINCT
  TO_BASE64(MD5(UPPER(email))) AS email_hash,
  MIN(created_At) OVER (PARTITION BY TO_BASE64(MD5(UPPER(email)))) AS first_purchase_date, 
FROM final
)

SELECT
  f.shopify_transaction_id,	
  TO_BASE64(MD5(UPPER(email))) AS email_hash,
  f.created_at AS created_at,
  item_title,
    CASE 
    WHEN REGEXP_CONTAINS(item_desc, r"\(SKU: (.*?)\)") 
    THEN REGEXP_EXTRACT(item_desc, r"\(SKU: (.*?)\)") 
    ELSE item_desc END 
  AS sku,
  variant_id,
  qty,
  order_item,
  s.shipping_method,
  s.shipping_country,
  SAFE_CAST(amount AS FLOAT64) AS order_item_price,
  SAFE_CAST(d.tax_rate AS FLOAT64) AS tax_rate,

  is_freegift,
  CASE 
    WHEN item_type != "set_item" 
        THEN pre_cart_discount ELSE 0 
    END AS pre_cart_discount,
  
  CASE 
    WHEN item_type != "set_item" 
        THEN item_coupon_discount 
        ELSE 0 
    END AS  item_coupon_discount,
  
  CASE 
    WHEN item_type != "set_item" 
        THEN discounted_item_price 
        ELSE 0 
    END AS discounted_item_price,

  code_value,
  coupon_code,
  code_type,
  item_desc,
  item_type,
  tags,
  order_note,	
  CASE WHEN f.created_at = c.first_purchase_date THEN 1 ELSE 0 END AS new_customer,
  CASE WHEN f.created_at = c.first_purchase_date THEN 0 ELSE 1 END AS returning_customer,
  CASE WHEN source_name = "580111" THEN "web" ELSE source_name END AS source_name,
  shop_order_ref,
  order_number
FROM final f
LEFT JOIN discounts d ON  d.transaction_id  = f.shopify_transaction_id and LID = d.lineitem_id 
LEFT JOIN tax t ON f.shopify_transaction_id = t.shopify_transaction_id
LEFT JOIN shipping s ON s.shopify_transaction_id = f.shopify_transaction_id
LEFT JOIN  first_purchase_date c ON c.email_hash = TO_BASE64(MD5(UPPER(f.email)))
WHERE item_title NOT LIKE '%_customerReferenceId' AND item_title NOT LIKE '_ll%'
ORDER BY f.shopify_transaction_id

{% endmacro %}