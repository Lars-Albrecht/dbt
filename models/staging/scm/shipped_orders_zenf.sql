SELECT
  SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
  SAFE_CAST(shipment_created_at AS TIMESTAMP) AS shipment_created_at,
  * EXCEPT(created_at,shipment_created_at)
FROM 
  `leslunes-prep.dbt_shipments.stg_shipments` 
WHERE 
  status = "SHIPPED"

/*
SELECT
  SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
  SAFE_CAST(shipment_created_at AS TIMESTAMP) AS shipment_created_at,
  * EXCEPT(shipping_address_name,
    updated_at,
    created_at,
    shipment_created_at),
  SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM
  `leslunes-raw.zenfulfillment.orders_de`
WHERE
  STATUS = "SHIPPED"
UNION ALL
SELECT
  SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
  SAFE_CAST(shipment_created_at AS TIMESTAMP) AS shipment_created_at,
  * EXCEPT(shipping_address_name,
    updated_at,
    created_at,
    shipment_created_at),
  SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM
  `leslunes-raw.zenfulfillment.orders_fr`
WHERE
  STATUS = "SHIPPED"
UNION ALL
SELECT
  SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
  SAFE_CAST(shipment_created_at AS TIMESTAMP) AS shipment_created_at,
  * EXCEPT(shipping_address_name,
    updated_at,
    created_at,
    shipment_created_at),
  SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM
  `leslunes-raw.zenfulfillment.orders_it`
WHERE
  STATUS = "SHIPPED"
UNION ALL
SELECT
  SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
  SAFE_CAST(shipment_created_at AS TIMESTAMP) AS shipment_created_at,
  * EXCEPT(shipping_address_name,
    updated_at,
    created_at,
    shipment_created_at,
    shipping_additional_details,
    shipping_remote_area),
  SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM
  `leslunes-raw.zenfulfillment.orders_xentral`
WHERE
  STATUS = "SHIPPED"
  */