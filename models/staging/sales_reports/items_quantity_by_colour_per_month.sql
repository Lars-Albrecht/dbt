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
    {{ sales_reports_line_items__sku('sales_report') }}
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
        WHEN SUBSTR(REGEXP_EXTRACT(line_items__sku, r'^[A-Za-z \d\W]+'),-3)='-2-' 
          THEN SUBSTR(REGEXP_EXTRACT(line_items__sku, r'^[A-Za-z \d\W]+'),1,LENGTH(REGEXP_EXTRACT(line_items__sku, r'^[A-Za-z \d\W]+'))-3)
        WHEN SUBSTR(REGEXP_EXTRACT(line_items__sku, r'^[A-Za-z \d\W]+'),-1)='-' 
          THEN SUBSTR(REGEXP_EXTRACT(line_items__sku, r'^[A-Za-z \d\W]+'),1,LENGTH(REGEXP_EXTRACT(line_items__sku, r'^[A-Za-z \d\W]+'))-1)
      ELSE
      REGEXP_EXTRACT(line_items__sku, r'^[A-Za-z \d\W]+')
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