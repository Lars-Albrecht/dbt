SELECT product_name,
 {% set col_dict = dbt_utils.get_query_results_as_dict("
WITH days AS (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT min(date(created_at)) FROM `leslunes-prep.dbt_orderitems.stg_orderitems_de`), current_date()+31, INTERVAL 1 month)) AS day
)
select DISTINCT(SUBSTR(SAFE_CAST(day AS STRING),1,7)) as day from days
") %}
{%- for col in col_dict.day %}
SUM(CASE
    WHEN created_at = '{{col}}' AND customer_type='old' THEN quantity
END
    ) AS `_{{col[0:4]}}_{{col[5:7]}}_old`,
SUM(CASE
    WHEN created_at = '{{col}}' AND customer_type='new' THEN quantity
END
    ) AS `_{{col[0:4]}}_{{col[5:7]}}_new`
{%-if not loop.last -%}
,
{%- endif -%}
{%- endfor %} 
   
   FROM ( 
    SELECT created_at, product_name, size, colour, SUM(line_items__quantity) AS quantity, CASE WHEN th_purchase=1 THEN 'new' else 'old' END AS customer_type FROM
    (SELECT
      ROW_NUMBER () OVER (PARTITION BY email_hash ORDER BY created_at) AS th_purchase,
      qty AS line_items__quantity,
      SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS created_at,
      sku AS line_items__sku
    FROM
    `leslunes-prep.dbt_orderitems.stg_orderitems_de`
    WHERE 
      source_name='web'
        AND created_at>='2020-08-01' 
        AND item_type!='set' ) AS orders
  LEFT JOIN (
    SELECT
      sku,
      product_name,
      size,
      colour
    FROM
      `leslunes-prep.dbt_prod_gsheets.masterlist`
      ) AS list
  ON
    orders.line_items__sku=list.sku 
    GROUP BY created_at, product_name, size, colour, customer_type
    ) AS A GROUP BY product_name
    ORDER BY product_name