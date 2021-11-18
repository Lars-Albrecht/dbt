WITH shipments AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY created_at, product_sku, reference, status ORDER BY shipment_created_at DESC) AS row_number
  FROM
   {{ source("zenf", "orders_de")}}
    )
SELECT
  _id,
  billing_city,
  billing_company,
  billing_country,
  billing_email,
  billing_name,
  billing_phone,
  billing_street,
  billing_street2,
  billing_region,
  billing_zip,
  created_at,
  product__id,
  product_barcode,
  product_price_excl_vat,
  product_price_incl_vat,
  product_quantity_ordered,
  product_quantity_refunded,
  product_quantity_returned,
  product_quantity_shipped,
  product_shop_product_id,
  product_shop_variant_id,
  product_sku,
  product_status,
  product_variant,
  reference,
  shipment_created_at,
  shipment_shipping_option,
  shipment_tracking_number,
  "" AS shipping_additional_details,
  shipping_address_city,
  shipping_address_company,
  shipping_address_country,
  shipping_address_email,
  shipping_address_name,
  shipping_address_phone,
  shipping_address_region,
  shipping_address_street,
  shipping_address_street2,
  shipping_address_zip,
  "" AS shipping_remote_area,
  shipping_subtotal,
  shipping_total,
  shop_order_id,
  shop_order_reference,
  status,
  store_id,
  updated_at
FROM
  shipments
WHERE row_number = 1
AND  status = "SHIPPED"