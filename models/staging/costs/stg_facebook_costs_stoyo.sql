SELECT *
FROM `leslunes-prep.dbt_prod_gsheets.fb_costs` 
WHERE date IS NOT NULL
ORDER BY date DESC
