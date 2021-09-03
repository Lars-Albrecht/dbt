{{ config(
    materialized='incremental',
    tags=["incremental"]
) }}

SELECT 
  *
FROM 
  `leslunes-raw.gsheets.ga_sessions_influencer_de`
WHERE date IS NOT NULL

{% if is_incremental() %}

-- this filter will only be applied on an incremental run
AND date > (select max(date) from {{ this }})

{% endif %}