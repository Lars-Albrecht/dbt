SELECT 
  year,
  month,
  CASE 
      WHEN MONTH = "January" THEN 1
      WHEN MONTH = "February" THEN 2
      WHEN MONTH = "March" THEN 3
      WHEN MONTH = "April" THEN 4
      WHEN MONTH = "May" THEN 5
      WHEN MONTH = "June" THEN 6
      WHEN MONTH = "July" OR MONTH = "Juli" THEN 7
      WHEN MONTH = "August" THEN 8
      WHEN MONTH = "September" THEN 9
      WHEN MONTH = "October" OR MONTH = "Oktober" THEN 10
      WHEN MONTH = "November" THEN 11
      WHEN MONTH = "December" THEN 12
    END AS month_int,
  SAFE_CAST(price_per_order AS FLOAT64) AS shipment_cost_per_order
FROM `leslunes-prep.dbt_prod_gsheets.stg_fulfillment_costs`
order by month_int