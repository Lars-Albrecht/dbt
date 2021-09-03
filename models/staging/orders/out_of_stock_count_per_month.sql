SELECT
  tags,
    {% set col_dict = dbt_utils.get_query_results_as_dict("
WITH days AS (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT min(date(created_at)) FROM `leslunes-prep.dbt_orders.orders_combined`), current_date()+31, INTERVAL 1 month)) AS day
)
select DISTINCT(SUBSTR(SAFE_CAST(day AS STRING),1,7)) as day from days
") %}
{%- for col in col_dict.day %}
SUM(CASE
    WHEN sales.order_date = '{{col}}' THEN sales.count_orders
END
    ) AS `sum_{{col[0:4]}}_{{col[5:7]}}`
{%-if not loop.last -%}
,
{%- endif -%}
{%- endfor %}

FROM (
  SELECT
    COUNT(DISTINCT name) AS count_orders,
    SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS order_date,
    tags
  FROM
    `leslunes-prep.dbt_orders.orders_with_items_combined` #`les-lunes-data-269915.import.unique_orders_with_items`
  WHERE
    tags IN ('Out-of-Stock 10% Erstattung, Out-of-Stock Order',
      'Out-of-Stock Order, Out-of-Stock Stornierung')
    AND source_name='web'
  GROUP BY
    order_date,
    tags
  UNION ALL
  SELECT
    COUNT(DISTINCT name) AS count_orders,
    SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS order_date,
    'Total Out-of-Stock Order' AS tags
  FROM
    `leslunes-prep.dbt_orders.orders_combined` #`les-lunes-data-269915.import.unique_orders`
  WHERE
    tags LIKE 'Out-of-Stock%'
    AND source_name='web'
  GROUP BY
    order_date ) AS sales
GROUP BY
  tags
ORDER BY
  tags