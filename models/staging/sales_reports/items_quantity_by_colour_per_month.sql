SELECT
  type,
  SUM(line_items__quantity) AS line_items__quantity,
  colour,
  {% set col_dict = dbt_utils.get_query_results_as_dict("
WITH days AS (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT min(date(created_at)) FROM `leslunes-prep.dbt_orderitems.stg_orderitems_combined`), current_date()+31, INTERVAL 1 month)) AS day
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
  FROM (SELECT 
    qty AS line_items__quantity,
    created_at,
    sku AS line_items__sku
FROM `leslunes-prep.dbt_orderitems.stg_orderitems_combined` 
WHERE 
    item_type!='set' 
        AND source_name='web') AS orders
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