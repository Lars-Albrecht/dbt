SELECT
  source,
  started_at,
  ROUND(SUM(cost),2) AS influencer_marketing_spent,
  coupon_code
FROM
  `leslunes-prep.dbt_marketing.deals_unique_casted_types_snapshot`
WHERE
  status='posted'
GROUP BY
  started_at,
  source,
  coupon_code
ORDER BY 
  started_at DESC