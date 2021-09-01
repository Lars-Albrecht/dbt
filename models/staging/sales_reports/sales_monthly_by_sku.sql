SELECT
  DATE_DIFF(CURRENT_DATE(), min_created_at, month) AS active_months,
  sales.line_items__sku,
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
    line_items__sku,
    SUM(SAFE_CAST(line_items__quantity AS INT64)) AS line_items__quantity,
    SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS order_date,
  FROM
    `leslunes-prep.dbt_orders.orders_with_items_combined` #`les-lunes-data-269915.import_combined.unique_orders_with_items`
  WHERE
    (source_name='web'
      OR UPPER(note) LIKE '%INFLUENCE%'
      OR UPPER(note) LIKE '%PRIORITY%'
      OR UPPER(tags) LIKE '%INFLUENCE%')
    AND line_items__sku!=''
    AND line_items__sku NOT LIKE '%fashion-bundle%'
  GROUP BY
    order_date,
    line_items__sku ) AS sales
LEFT JOIN (
  SELECT
    line_items__sku,
    CAST(MIN(SUBSTR(SAFE_CAST(created_at AS STRING),1,10)) AS DATE) AS min_created_at
  FROM
    `leslunes-prep.dbt_orders.orders_with_items_combined` #`les-lunes-data-269915.import_combined.unique_orders_with_items`
  WHERE
    (source_name='web'
      OR UPPER(note) LIKE '%INFLUENCE%'
      OR UPPER(note) LIKE '%PRIORITY%'
      OR UPPER(tags) LIKE '%INFLUENCE%')
    AND line_items__sku!=''
    AND line_items__sku NOT LIKE '%fashion-bundle%'
  GROUP BY
    line_items__sku) AS active_months
ON
  sales.line_items__sku=active_months.line_items__sku
GROUP BY
  line_items__sku,
  active_months
ORDER BY
  line_items__sku
