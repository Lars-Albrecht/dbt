SELECT
  id,
  email,
  created_at,
  updated_at,
  number,
  name,
  note,
  token,
  gateway,
  test,
  total_price,
  subtotal_price,
  total_tax,
  tax_rate,
  financial_status,
  fulfillment_status,
  confirmed,
  total_discounts,
  total_line_items_price,
  cancelled_at,
  cancel_reason,
  discount_codes__amount,
  discount_codes,
  discount_codes__type,
  discount_value,	
  source_name,
  tags,
  line_items__id,
  line_items__variant_id,
  line_items__title,
  line_items__quantity,
  line_items__sku,
  line_items__product_id,
  line_items__name,
  line_items__price,
  line_items__price_all,
  line_items__total_discount,
  line_items__total_discount_all,
  line_items__fulfillment_status,
  order__id,
  #line_items_properties__name,
  #line_items_properties__value,
  item_type,
  refund_amount,
  refunded_at,
  checkout_id,
  filfillment_updated_at, 
  status,
  shipping_address__country,
  total_shipping_price_set__presentment_money__amount AS shippment_amount,
  'DE' AS source
FROM
  `leslunes-prep.orders.unique_orders_with_items_de`
UNION ALL
SELECT
  id,
  email,
  created_at,
  updated_at,
  number,
  name,
  note,
  token,
  gateway,
  test,
  total_price,
  subtotal_price,
  total_tax,
  tax_rate,
  financial_status,
  fulfillment_status,
  confirmed,
  total_discounts,
  total_line_items_price,
  cancelled_at,
  cancel_reason,
  discount_codes__amount,
  discount_codes,
  discount_codes__type,
  discount_value,
  source_name,
  tags,
  line_items__id,
  line_items__variant_id,
  line_items__title,
  line_items__quantity,
  line_items__sku,
  line_items__product_id,
  line_items__name,
  line_items__price,
  line_items__price_all,
  line_items__total_discount,
  line_items__total_discount_all,
  line_items__fulfillment_status,
  order__id,
  #line_items_properties__name,
  #line_items_properties__value,
  item_type,
  refund_amount,
  refunded_at,
  checkout_id,
  filfillment_updated_at, 
  status,
  shipping_address__country,
  total_shipping_price_set__presentment_money__amount AS shippment_amount,
  'FR' AS source
FROM
  `leslunes-prep.orders.unique_orders_with_items_fr`
UNION ALL
SELECT
  id,
  email,
  created_at,
  updated_at,
  number,
  name,
  note,
  token,
  gateway,
  test,
  total_price,
  subtotal_price,
  total_tax,
  tax_rate,
  financial_status,
  fulfillment_status,
  confirmed,
  total_discounts,
  total_line_items_price,
  cancelled_at,
  cancel_reason,
  discount_codes__amount,
  discount_codes,
  discount_codes__type,
  discount_value,
  source_name,
  tags,
  line_items__id,
  line_items__variant_id,
  line_items__title,
  line_items__quantity,
  line_items__sku,
  line_items__product_id,
  line_items__name,
  line_items__price,
  0.0 AS line_items__price_all,
  line_items__total_discount,
  0.0 AS line_items__total_discount_all,
  line_items__fulfillment_status,
  order__id,
  #line_items_properties__name,
  #line_items_properties__value,
  item_type,
  refund_amount,
  refunded_at,
  checkout_id,
  filfillment_updated_at, 
  status,
  shipping_address__country,
  total_shipping_price_set__presentment_money__amount AS shippment_amount,
  'IT' AS source
FROM
  `leslunes-prep.orders.unique_orders_with_items_it`