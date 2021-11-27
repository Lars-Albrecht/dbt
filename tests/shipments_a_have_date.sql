SELECT 
  *
FROM
  `leslunes-prep.dbt_shipments.stg_shipments`
WHERE 
  shipment_created_at = "None"
AND 
  status = "SHIPPED"