SELECT
  product_sku
  , CASE
  {{ sales_reports_line_items__sku('returns_report') }}
    ELSE product_sku END AS metabase_line_style_name
  , shipping_address_country
  , style
  , sc.size_ll  as size
  , color
  , items.category AS sku_category
  , CASE WHEN pm.category IS NULL THEN os.category ELSE pm.category END AS product_category
  
  ,CASE 
    WHEN regexp_contains(product_sku, r'LANA') THEN 'BA'
    WHEN regexp_contains(product_sku, r'PAUL') THEN 'BA'
    WHEN regexp_contains(product_sku, r'(04-BA-STELLA|WSTELL125|WSTELLA125).*(XSP|XS\/S|S\/M|M\/L|L\/XL|M\/L)') THEN 'BA'
    WHEN regexp_contains(product_sku, r'ALEN') THEN 'BA'
    WHEN regexp_contains(product_sku, r'STEV') THEN 'BA'
    WHEN regexp_contains(product_sku, r'ROBIN') THEN 'BA'
    ELSE material 
   END AS material

  , pm.active_vs_not_active 
  , SUM(CASE WHEN products_returned = "yes" THEN 1 ELSE 0 END)  AS items_returned
  , count(*) AS items_shipped
  , ordered_at
  , ordered_at_month
  , ordered_at_year
  , shop
  , CONCAT(ordered_at_year,'-',ordered_at_month) AS ordered_at_year_and_month
  , sum(one) AS too_big
  , sum(two) AS too_small
  , sum(three) AS too_long
  , sum(four) AS too_short
  , sum(five) AS quality_not_like_expected
  , sum(six) AS material_not_like_expected
  , sum(seven) AS fit_does_not_suit_me
  , sum(eight) AS damaged_defective
  , sum(nine) AS several_colors_or_sizes_ordered
  , sum(ten) AS article_differs_from_website
  , sum(eleven) AS late_delivery_wrong_item
  , sum(twelve) AS others
  , sum(one)+sum(two)+sum(three)+sum(four)+sum(five)+sum(six)+sum(seven)+sum(eight)+sum(nine)+sum(ten)+sum(eleven)+sum(twelve) AS all_reasons_count
FROM `leslunes-prep.dbt_returns.unique_returned_items` items
LEFT JOIN `leslunes-prep.dbt_gsheets.size-config` sc on sc.size_ll = size
LEFT JOIN `leslunes-prep.dbt_products.masterlist` pm on UPPER(pm.sku) = UPPER(product_sku)
LEFT JOIN `leslunes-raw.products.old_skus` os on UPPER(os.sku) = UPPER(product_sku)
GROUP BY 1,2,3,4,5,6,7,8,9,10,13,14,15,16
