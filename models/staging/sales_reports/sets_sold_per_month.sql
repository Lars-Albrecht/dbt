SELECT
  source AS shop,
  line_items__name AS set_name,
  SUM(line_items__quantity) AS sum_quantity,
{% set col_dict = dbt_utils.get_query_results_as_dict("
WITH days AS (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT min(date(created_at)) FROM `leslunes-prep.dbt_orders.orders_with_items_combined`), current_date()+31, INTERVAL 1 month)) AS day
)
select DISTINCT(SUBSTR(SAFE_CAST(day AS STRING),1,7)) as day from days
") %}
{%- for col in col_dict.day %}
SUM(CASE
      WHEN SUBSTR(SAFE_CAST(created_at AS STRING),1,7) LIKE '%{{col}}%' THEN line_items__quantity
END
    ) AS `_{{col[0:4]}}_{{col[5:7]}}`
{%-if not loop.last -%}
,
{%- endif -%}
{%- endfor %}
FROM
  `leslunes-prep.dbt_orders.orders_with_items_combined` #import_combined.unique_orders_with_items
WHERE
  source_name='web'
  AND item_type='SET'
GROUP BY
  shop,
  line_items__name,
  item_type
ORDER BY
  shop,
  item_type,
  line_items__name