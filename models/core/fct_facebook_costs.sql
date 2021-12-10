WITH old_deal AS(
    # facebook budget spend by Stoyo agency
SELECT 
    date, 
    payout_fb AS spend 
FROM 
    `leslunes-prep.dbt_costs.stg_facebook_costs_stoyo`
WHERE 
    date <= "2021-06-06"
),

new_deal AS (
SELECT 
    date, 
    payout_code_redemptions AS spend
FROM 
    `leslunes-prep.dbt_costs.stg_facebook_costs_stoyo`
WHERE 
    date > "2021-06-06"
), 

own_try AS(
SELECT 
    date, 
    total_spend AS spend
FROM 
    `leslunes-prep.dbt_costs.stg_facebook_costs`
)

SELECT * FROM old_deal
UNION ALL
SELECT * FROM new_deal
UNION ALL
SELECT * FROM own_try
ORDER BY DATE DESC