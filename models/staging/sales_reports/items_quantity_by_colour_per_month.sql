SELECT
  type,
  SUM(line_items__quantity) AS line_items__quantity,
  colour,
  {% set col_dict = dbt_utils.get_query_results_as_dict("
WITH days AS (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT min(date(created_at)) FROM `leslunes-prep.dbt_orders.orders_with_items_combined`), current_date()+31, INTERVAL 1 month)) AS day
)
select DISTINCT(SUBSTR(SAFE_CAST(day AS STRING),1,7)) as day from days
") %}
{%- for col in col_dict.day %}
SUM(CASE
    WHEN sales.order_date = '{{col}}' THEN sales.line_items__quantity
END
    ) AS `_{{col[0:4]}}_{{col[5:7]}}`
{%-if not loop.last -%}
,
{%- endif -%}
{%- endfor %}

FROM (
  SELECT
    (CASE
        WHEN line_items__sku LIKE '%4007%' OR UPPER(line_items__sku) LIKE '%HWLEG%' THEN 'High Waisted Leggings'
        WHEN line_items__sku LIKE '%WPAUL125%' OR line_items__sku LIKE '%BA-PAUL%' THEN 'Paul'
        WHEN line_items__sku LIKE '%WSTELL125%' OR line_items__sku LIKE '%WSTELLA125%' OR line_items__sku LIKE '%STELLA%' THEN 'Stella'
        WHEN line_items__sku LIKE '%ALEN%' THEN 'Alena'
        WHEN line_items__sku LIKE '%STEV%' THEN 'Steve'
        WHEN line_items__sku LIKE '%MICHELLE-ECR%' OR line_items__sku LIKE '%ECR-MICHE%' THEN 'Michelle Rib'
        WHEN line_items__sku LIKE '%MICHELLE%' OR line_items__sku LIKE '%EC-MICHEL%' THEN 'Michelle' 
        WHEN line_items__sku LIKE '%OLIVIA%' THEN 'Olivia'
        WHEN line_items__sku LIKE '%CHARLOTTE%' OR line_items__sku LIKE '%CHARLT%' THEN 'Charlotte'
        WHEN line_items__sku LIKE '%ROBIN%' THEN 'Robin'
        WHEN line_items__sku LIKE '%GRACE%' THEN 'Grace'
        WHEN line_items__sku LIKE '%SOPHIA%' THEN 'Sophia'
        WHEN line_items__sku LIKE '%EMMA%' THEN 'Emma'
        WHEN line_items__sku LIKE '%LANA%' THEN 'Lana'
        WHEN line_items__sku LIKE '%AVA%' THEN 'Ava'
        WHEN line_items__sku LIKE '%MIA%' THEN 'Mia'
        WHEN line_items__sku LIKE '%JADE%' THEN 'Jade'
        WHEN line_items__sku LIKE '%MILEY%' THEN 'Miley Mask'
        WHEN line_items__sku LIKE '%RUBY%' THEN 'Ruby Scrunchie'
        WHEN line_items__sku LIKE '%VALERY%' THEN 'The Valery Scarf'
        WHEN line_items__sku LIKE '%LUNA%' THEN 'Luna Leggings'
        WHEN line_items__sku LIKE '%ABBY%' THEN 'Abby'
        WHEN line_items__sku LIKE  '%CARA2% 'THEN 'Cara_2'
        WHEN line_items__sku LIKE '%CARA%' THEN 'Cara'
        WHEN line_items__sku LIKE '%LOU%' THEN 'Lou'
        WHEN line_items__sku LIKE '%BELLE%' THEN 'Belle'
        WHEN line_items__sku LIKE '%DAISY%' THEN 'Daisy'
        WHEN line_items__sku LIKE '%LILY%' THEN 'Lily'
        WHEN line_items__sku LIKE '%RUBY%' THEN 'Ruby Scrunchie'
        WHEN line_items__sku LIKE '%HALEY%' THEN 'Haley'
        WHEN line_items__sku LIKE '%HARPER%' THEN 'Harper'
        WHEN line_items__sku LIKE '%ROB%' THEN 'Rob'
        WHEN line_items__sku LIKE '%APRIL%' THEN 'April'
        WHEN line_items__sku LIKE '%IVY%' THEN 'Ivy'
        WHEN line_items__sku LIKE '%ZOE%' THEN 'Zoey'
        WHEN line_items__sku LIKE '%LEO%' THEN 'Leo'
        WHEN line_items__sku LIKE '%CHARLIE%' THEN 'Charlie'
        WHEN line_items__sku LIKE '%NOLA%' THEN 'Nola'
        WHEN line_items__sku LIKE '%ROSIE%' THEN 'Rosie'
      ELSE
      'Other'
    END
      ) AS type,
    line_items__sku,
    SUM(SAFE_CAST(line_items__quantity AS INT64)) AS line_items__quantity,
    SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS order_date,
    colour
  FROM (
    SELECT
      line_items__name,
      line_items__quantity,
      created_at,
      CASE
        WHEN SUBSTR(REGEXP_EXTRACT(line_items__sku, r'^[A-Z \d\W]+'),-3)='-2-' 
          THEN SUBSTR(REGEXP_EXTRACT(line_items__sku, r'^[A-Z \d\W]+'),1,LENGTH(REGEXP_EXTRACT(line_items__sku, r'^[A-Z \d\W]+'))-3)
        WHEN SUBSTR(REGEXP_EXTRACT(line_items__sku, r'^[A-Z \d\W]+'),-1)='-' 
          THEN SUBSTR(REGEXP_EXTRACT(line_items__sku, r'^[A-Z \d\W]+'),1,LENGTH(REGEXP_EXTRACT(line_items__sku, r'^[A-Z \d\W]+'))-1)
      ELSE
      REGEXP_EXTRACT(line_items__sku, r'^[A-Z \d\W]+')
    END
      AS line_items__sku
    FROM
     `leslunes-prep.dbt_orders.orders_with_items_combined` #`les-lunes-data-269915.import_combined.unique_orders_with_items`
    WHERE
      (source_name='web')
      AND line_items__sku!='' AND UPPER(line_items__sku) NOT LIKE '%SET%') AS orders
  LEFT JOIN (
    SELECT
      sku,
      product_name,
      size,
      colour
    FROM
      `leslunes-prep.dbt_prod_gsheets.masterlist`  #import.products_masterlist
      ) AS list
  ON
    orders.line_items__sku=list.sku
  GROUP BY
    order_date,
    line_items__sku,
    type,
    colour ) AS sales
GROUP BY
  type,
  colour
ORDER BY
  type,
  colour