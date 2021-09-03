SELECT transaction_id, amount, created_at, gateway, 'DE' AS source 
FROM {{ ref('stg_refunds_amount_per_order_de') }}
UNION ALL
SELECT transaction_id, amount, created_at,gateway, 'FR' AS source  
FROM {{ ref('stg_refunds_amount_per_order_fr') }}
UNION ALL
SELECT transaction_id, amount, created_at,gateway, 'IT' AS source  
FROM {{ ref('stg_refunds_amount_per_order_it') }}