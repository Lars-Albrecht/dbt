SELECT
  source AS shop,
  'Amount' AS measure,
  refund_type,
    {% set col_dict = dbt_utils.get_query_results_as_dict("
WITH days AS (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT min(date(created_at)) FROM `leslunes-prep.dbt_orders.orders_with_items_combined`), current_date()+31, INTERVAL 1 month)) AS day
)
select DISTINCT(SUBSTR(SAFE_CAST(day AS STRING),1,7)) as day from days
") %}
{%- for col in col_dict.day %}
SUM(CASE
    WHEN created_at = '{{col}}' THEN refunded_amount
END
    ) AS `_{{col[0:4]}}_{{col[5:7]}}`
{%-if not loop.last -%}
,
{%- endif -%}
{%- endfor %}

FROM (
  SELECT
    source,
    created_at,
    refund_type,
    ROUND(SUM(refund_amount-refund_tax),2) AS refunded_amount
  FROM (
    SELECT
      source,
      SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS created_at,
      id,
      fulfillment_status,
      note,
      tags,
      refund_amount,
      total_price,
      CASE
        WHEN total_price=0 OR total_price IS NULL THEN 0
      ELSE
      (total_tax*refund_amount)/total_price
    END
      AS refund_tax,
      CASE
        WHEN tags NOT LIKE '%Out-of-Stock 10%' AND fulfillment_status='fulfilled' THEN 'Usual refund'
        WHEN tags NOT LIKE '%Out-of-Stock 10%'
      AND (fulfillment_status!='fulfilled'
        OR fulfillment_status IS NULL) THEN 'Cancelation'
        WHEN tags LIKE '%Out-of-Stock 10%' AND ROUND(total_price/10,0)!=ROUND(refund_amount,0) AND fulfillment_status='fulfilled' THEN 'Usual refund'
        WHEN tags LIKE '%Out-of-Stock 10%'
      AND ROUND(total_price/10,0)!=ROUND(refund_amount,0)
      AND (fulfillment_status!='fulfilled'
        OR fulfillment_status IS NULL) THEN 'Cancelation'
        WHEN tags LIKE '%Out-of-Stock 10%' THEN 'Out-of-Stock 10'
      ELSE
      'Other'
    END
      AS refund_type
    FROM
       `leslunes-prep.dbt_orders.orders_with_items_combined` #import_combined.unique_orders_with_items
    WHERE
      source_name='web'
      AND refund_amount>0
    GROUP BY
      source,
      created_at,
      id,
      fulfillment_status,
      note,
      tags,
      refund_amount,
      total_price,
      total_tax) AS A
  GROUP BY
    source,
    created_at,
    refund_type)
GROUP BY
  source,
  refund_type
UNION ALL
SELECT
  source AS shop,
  'Quantity' AS measure,
  refund_type,
      {% set col_dict = dbt_utils.get_query_results_as_dict("
WITH days AS (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT min(date(created_at)) FROM `leslunes-prep.dbt_orders.orders_with_items_combined`), current_date()+31, INTERVAL 1 month)) AS day
)
select DISTINCT(SUBSTR(SAFE_CAST(day AS STRING),1,7)) as day from days
") %}
{%- for col in col_dict.day %}
SUM(CASE
    WHEN created_at = '{{col}}' THEN refunded_quantity
END
    ) AS `_{{col[0:4]}}_{{col[5:7]}}`
{%-if not loop.last -%}
,
{%- endif -%}
{%- endfor %}

FROM (
  SELECT
    source,
    created_at,
    refund_type,
    COUNT(DISTINCT id) AS refunded_quantity
  FROM (
    SELECT
      source,
      SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS created_at,
      id,
      fulfillment_status,
      note,
      tags,
      refund_amount,
      total_price,
      CASE
        WHEN tags NOT LIKE '%Out-of-Stock 10%' AND fulfillment_status='fulfilled' THEN 'Usual refund'
        WHEN tags NOT LIKE '%Out-of-Stock 10%'
      AND (fulfillment_status!='fulfilled'
        OR fulfillment_status IS NULL) THEN 'Cancelation'
        WHEN tags LIKE '%Out-of-Stock 10%' 
        AND ROUND(total_price/10,0)!=ROUND(refund_amount,0) AND fulfillment_status='fulfilled' THEN 'Usual refund'
        WHEN tags LIKE '%Out-of-Stock 10%'
      AND ROUND(total_price/10,0)!=ROUND(refund_amount,0)
      AND (fulfillment_status!='fulfilled'
        OR fulfillment_status IS NULL) THEN 'Cancelation'
        WHEN tags LIKE '%Out-of-Stock 10%' THEN 'Out-of-Stock 10'
      ELSE
      'Other'
    END
      AS refund_type
    FROM
      `leslunes-prep.dbt_orders.orders_with_items_combined` #import_combined.unique_orders_with_items
    WHERE
      source_name='web'
      AND refund_amount>0
    GROUP BY
      source,
      created_at,
      id,
      fulfillment_status,
      note,
      tags,
      refund_amount,
      total_price) AS A
  GROUP BY
    source,
    created_at,
    refund_type)
GROUP BY
  source,
  refund_type
ORDER BY
  shop,
  measure,
  refund_type