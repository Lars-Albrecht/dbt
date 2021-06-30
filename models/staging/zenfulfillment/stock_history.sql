SELECT  
    report_id, 
    user_id, 
    storage_location, 
    PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S %z %Z", created_at) AS created_at,
    PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S %z %Z", processed_at) AS processed_at,
    customer_company_name, 
    status, 
    stock_taking_id, 
    le_id, 
    container_id_warehouse, 
    storage_area, 
    shop_variant_id, 
    variant_id, 
    article_name,
    REGEXP_REPLACE(REGEXP_EXTRACT(article_name, r"^[\w\W]+?(?:\s-\s)"), r".{3}$","") as style,
    SAFE_CAST(REGEXP_REPLACE(stored_at, r"[+]\d{4}\D{4}", "UTC") AS TIMESTAMP) AS stored_at, 
    SAFE_CAST(REGEXP_REPLACE(last_stock_taking, r"[+]\d{4}\D{4}", "UTC") AS TIMESTAMP) AS last_stock_taking, 
    inbound_id, 
    nve, 
    mhd, 
    charge, 
    unit_type, 
    SAFE_CAST(available_stock AS INT64) AS available_stock, 
    SAFE_CAST(blocked_stock AS INT64) AS blocked_stock 
FROM `leslunes-raw.zenfulfillment.stock_history`