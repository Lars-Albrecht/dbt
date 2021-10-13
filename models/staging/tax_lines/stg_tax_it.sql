WITH tax_lines AS(
SELECT 
    o.id AS transaction_id,		
    tl.value.price AS tax_amount,	
    tl.value.rate AS tax_rate,
    tl.value.title As tax_title,
    row_number() OVER(PARTITION BY o.id,tl.value.price,tl.value.rate) as row_number
FROM leslunes-raw.shopify_it.orders o
LEFT JOIN UNNEST(tax_lines) AS tl
)

SELECT 
    transaction_id,
    sum(tax_amount) as tax_amount,
    avg(tax_rate) AS tax_rate,
    tax_title
FROM tax_lines 
WHERE row_number = 1
GROUP BY 1,4