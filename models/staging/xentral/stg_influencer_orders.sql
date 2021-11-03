SELECT 
    SAFE_CAST(Datum AS DATE) AS Datum
    ,CAST(Internet AS STRING) AS Internet
    ,Status
    ,Name
    ,CAST(Belegnr AS STRING) AS Belegnr
    ,Bezeichnung	
    ,Nummer
    ,Internetseite
    ,Bearbeiter
FROM {{ source('xentral', 'influencer_orders') }}
UNION ALL
SELECT 
    SAFE_CAST(Datum AS DATE) AS Datum
    ,Internet
    ,CASE WHEN Status not in ('storniert', 'abgeschlossen') THEN "unknown" ELSE Status END AS Status
    ,Name
    ,Belegnr
    ,Bezeichnung	
    ,Nummer
    ,Internetseite
    ,Bearbeiter
FROM {{ source('xentral', 'influencer_orders_old') }}