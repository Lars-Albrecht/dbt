WITH CTE AS (
### DE
SELECT 
  shipment_created_at AS created_at,
  o.product_sku,
  products_qty,
  products_condition_note,
  shipping_address_country,
  re.products_returned,
  "DE" AS shop
FROM `leslunes-prep.dbt_shipments.stg_shipments_de` o
LEFT JOIN `leslunes-prep.dbt_returns.stg_returns_de` re on re.order_reference = o.reference AND re.products_sku = o.product_sku 
WHERE o.status = "SHIPPED"
UNION ALL
### XENTRAL
SELECT 
  shipment_created_at AS created_at,
  o.product_sku,
  products_qty,
  products_condition_note,
  shipping_address_country,
  IFNULL(re.products_returned, "no") AS products_returned,
  o.shop_order_reference AS shop
FROM 
  `leslunes-prep.dbt_shipments.stg_shipments_xentral` o
LEFT JOIN 
  `leslunes-prep.dbt_returns.stg_returns_xentral` re ON re.order_reference = o.reference AND re.products_sku = o.product_sku 
WHERE 
  # exclude influencer orders
  o.shop_order_reference NOT LIKE "INF%" AND o.status = "SHIPPED"
AND shipment_created_at != "None" 
### FR
UNION ALL
SELECT 
  shipment_created_at AS created_at,
  o.product_sku,
  products_qty,
  products_condition_note,
  shipping_address_country,
  re.products_returned,
  "FR" as source
FROM 
  `leslunes-prep.dbt_shipments.stg_shipments_fr` o
LEFT JOIN 
  `leslunes-prep.dbt_returns.stg_returns_fr` re ON re.order_reference = o.reference AND re.products_sku = o.product_sku 
WHERE o.status = "SHIPPED"
### IT
UNION ALL
SELECT 
  shipment_created_at AS created_at,
  o.product_sku,
  products_qty,
  products_condition_note,
  shipping_address_country,
  re.products_returned,
  "IT" as shop
FROM `leslunes-prep.dbt_shipments.stg_shipments_it` o
LEFT JOIN `leslunes-prep.dbt_returns.stg_returns_it` re on re.order_reference = o.reference AND re.products_sku = o.product_sku 
WHERE o.status = "SHIPPED"
)

SELECT 
CASE 
WHEN product_sku like "%-old-duplicate" THEN REPLACE(product_sku, "-old-duplicate","")
WHEN product_sku like "%-duplicate" THEN REPLACE(product_sku, "-duplicate","") 
WHEN product_sku like "%-old" THEN REPLACE(product_sku, "-old","") 
ELSE product_sku
END AS product_sku,
products_condition_note,	
shipping_address_country,
products_returned
#* EXCEPT(created_at, products_qty, shop)

,CASE 
WHEN created_at LIKE '%T%' THEN PARSE_DATE("%Y-%m-%d", SPLIT(created_at,"T")[SAFE_OFFSET(0)]) 
WHEN created_at LIKE '% %' THEN PARSE_DATE("%Y-%m-%d", SPLIT(created_at," ")[SAFE_OFFSET(0)])
END AS ordered_at

,CASE
WHEN created_at LIKE '%T%' THEN EXTRACT(MONTH FROM PARSE_DATE("%Y-%m-%d", SPLIT(created_at,"T")[SAFE_OFFSET(0)]))
WHEN created_at LIKE '% %' THEN EXTRACT(MONTH FROM PARSE_DATE("%Y-%m-%d", SPLIT(created_at," ")[SAFE_OFFSET(0)]))
END AS ordered_at_month

,CASE
WHEN created_at LIKE '%T%' THEN EXTRACT(YEAR FROM PARSE_DATE("%Y-%m-%d", SPLIT(created_at,"T")[SAFE_OFFSET(0)]))
WHEN created_at LIKE '% %' THEN EXTRACT(YEAR FROM PARSE_DATE("%Y-%m-%d", SPLIT(created_at," ")[SAFE_OFFSET(0)])) 
END AS ordered_at_year

,CASE
     WHEN product_sku like "0%" THEN SPLIT(product_sku, "-")[SAFE_OFFSET(2)]
     WHEN regexp_extract(product_sku, "^(\\w*)") = "W" THEN LEFT(product_sku,5)
     WHEN regexp_extract(product_sku, "^(\\w*)") like "INFLUENCERCARD_%" THEN "n/a" 
     ELSE regexp_extract(product_sku, "^(\\w*)") 
  END as style,
  CASE
    WHEN product_sku = "08-ECR-ROSIE-BGmM" THEN "M" ### workaround because of malformed SKU
    WHEN product_sku like "0%" THEN SPLIT(product_sku, "-")[SAFE_OFFSET(4)]
    WHEN regexp_extract(product_sku, "^(\\w*)") = "W" THEN regexp_extract(product_sku,"(\\w*\\/\\w*)$")
    WHEN regexp_extract(product_sku, "-([A-Z/0-9]*)-\\w\\/?\\w*$") = "EC" THEN "UNI"
    WHEN regexp_extract(product_sku, "^(\\w*)") like "INFLUENCERCARD_%" THEN "n/a" 
  ELSE regexp_extract(product_sku, "-([A-Z/0-9]*)-\\w\\/?\\w*$") END AS size,
  ### extract and transform color from skus like W-219-ALEN-07-01-100-101-260-100-XS/S where color is in the 3-digits next to size (at the end)
  CASE 
  WHEN product_sku = "08-ECR-ROSIE-BGmM" THEN "BGm" ### workaround because of malformed SKU
  WHEN product_sku like "0%" THEN SPLIT(product_sku, "-")[SAFE_OFFSET(3)]
  WHEN regexp_extract(product_sku, "^(\\w*)") = "W" THEN (
                          SELECT color 
                          from (SELECT "100" as code, "WH" as color
                                UNION ALL
                                SELECT "801" as code, "HG" as color
                                UNION ALL
                                SELECT "999" as code, "BK" as color) 
                                where code = regexp_extract(product_sku,"(\\d{3})-\\w*\\/\\w*$")
                          ) 
  WHEN regexp_extract(product_sku, "^(\\w*)") like "INFLUENCERCARD_%" THEN "n/a" 
  ELSE regexp_extract(product_sku, "(\\w*)$")
  END as color
  ,IFNULL(SAFE_CAST(products_qty AS INT64), 0) AS items_returned
  ,CASE
     WHEN product_sku like "0%" THEN SPLIT(product_sku, "-")[SAFE_OFFSET(0)] ELSE "" END AS category
  ,CASE
     WHEN product_sku like "0%" THEN SPLIT(product_sku, "-")[SAFE_OFFSET(1)] 
     WHEN REGEXP_CONTAINS(product_sku, r"^[[:upper:]].*(EC|ECR|BA|CO|MDR|OCO)") THEN SPLIT(product_sku,"-")[SAFE_OFFSET(1)]
     ELSE "" END AS material
  ,CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "1" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"(12)") = TRUE THEN 0
    WHEN REGEXP_CONTAINS(products_condition_note, r"(11)") = TRUE THEN 0
    WHEN REGEXP_EXTRACT(products_condition_note, r"([0-9]{1,1})") = "1" THEN 1
    WHEN REGEXP_EXTRACT(products_condition_note, r"^[0-9]{1}") = "1"  THEN 1
    ELSE 0
  END as one,
   CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "2" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"(12)") = TRUE THEN 0
    WHEN "2" in UNNEST(SPLIT(products_condition_note, ",")) THEN 1
    WHEN "2" in UNNEST(SPLIT(products_condition_note, "/")) THEN 1
    WHEN "2" in UNNEST(SPLIT(products_condition_note, ".")) THEN 1
     ELSE 0
   END as two, 
   CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "3" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"3") = TRUE THEN 1
     ELSE 0
   END as three,
      CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "4" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"4") = TRUE THEN 1
     ELSE 0
   END as four,
   CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "5" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"5") = TRUE THEN 1
     ELSE 0
   END as five,
   CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN "6" in UNNEST(SPLIT(products_condition_note, ",")) THEN 1
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "6" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r",?6,?") = TRUE THEN 1
     ELSE 0
   END as six,
      CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "7" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"7") = TRUE THEN 1
     ELSE 0
   END as seven,
      CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "8" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"8") = TRUE THEN 1
     ELSE 0
   END as eight,
      CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "9" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"9") = TRUE THEN 1
     ELSE 0
   END as nine,
      CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "10" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"10") = TRUE THEN 1
    ELSE 0
   END as ten,
   CASE
    WHEN LENGTH(products_condition_note) > 32 THEN 0
    WHEN SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "11" THEN 1
    WHEN REGEXP_CONTAINS(products_condition_note, r"11") = TRUE THEN 1
     ELSE 0
   END as eleven,
      CASE
    WHEN products_returned = "yes" AND LENGTH(products_condition_note) > 32 THEN 1
    WHEN products_returned = "yes" AND SPLIT(products_condition_note, ".")[SAFE_OFFSET(0)] = "12" THEN 1
    WHEN products_returned = "yes" AND REGEXP_CONTAINS(products_condition_note, r"[^0-9.,/+]") = TRUE THEN 1
    WHEN products_returned = "yes" AND REGEXP_CONTAINS(products_condition_note, r"12") = TRUE THEN 1
     ELSE 0
   END as twelve,
   CASE
      WHEN REGEXP_CONTAINS(shop, r"IT") THEN "IT" 
      WHEN REGEXP_CONTAINS(shop, r"CS") THEN "CS"
      WHEN REGEXP_CONTAINS(shop, r"FR") THEN "FR"
      WHEN REGEXP_CONTAINS(shop, r"^#\d*") THEN "DE"
      WHEN REGEXP_CONTAINS(shop, r"^\d*") THEN "DE"
      ELSE shop
    END AS shop
FROM CTE
WHERE product_sku IS NOT NULL