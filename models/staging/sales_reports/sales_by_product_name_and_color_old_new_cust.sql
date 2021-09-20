SELECT product_name, colour,
{% set col_dict = dbt_utils.get_query_results_as_dict("
WITH days AS (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT min(date(created_at)) FROM `leslunes-prep.dbt_orders.unique_orders_with_items_de`), current_date()+31, INTERVAL 1 month)) AS day
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
      ROW_NUMBER () OVER (PARTITION BY email /*, source*/ ORDER BY created_at) AS th_purchase,
      line_items__name,
      line_items__quantity,
      SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS created_at,
      line_items__sku as line,
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
     `leslunes-prep.dbt_orders.unique_orders_with_items_de`
    WHERE
      (source_name='web')
      AND line_items__sku!='' 
      AND created_at>='2020-08-01' AND UPPER(line_items__name) NOT LIKE '%SET%') AS orders
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
    ) AS A GROUP BY product_name, colour
    ORDER BY product_name, colour