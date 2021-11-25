SELECT
  * EXCEPT(shipping_address_name,
    updated_at),
  SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM
  `leslunes-raw.zenfulfillment.orders_de`
UNION ALL
SELECT
  * EXCEPT(shipping_address_name,
    updated_at),
  SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM
  `leslunes-raw.zenfulfillment.orders_fr`
UNION ALL
SELECT
  * EXCEPT(shipping_address_name,
    updated_at),
  SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM
  `leslunes-raw.zenfulfillment.orders_it`
UNION ALL
SELECT
  * EXCEPT(shipping_address_name,
    updated_at, shipping_additional_details, shipping_remote_area),
  SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM
  `leslunes-raw.zenfulfillment.orders_xentral`
WHERE
  SAFE_CAST(updated_at AS TIMESTAMP) IS NOT NULL