SELECT 
  code, influencerfullName,
CASE 
     WHEN influencerfullName='leslunes.de' THEN 'OWN_IG'
     WHEN influencerfullName='leslunes.fr' THEN 'OWN_IG_FR'
     WHEN influencerfullName='leslunes.it' THEN 'OWN_IG_IT'
     WHEN UPPER(influencerfullName) LIKE '%LES LUNES CUSTOMER CARE%' THEN 'CS'
     WHEN UPPER(influencerfullName) LIKE '%LESLUNES NEWSLETTER%' THEN 'NL'
     WHEN UPPER(influencerfullName) LIKE '%LL FR NL%' THEN 'NL_FR'
     WHEN UPPER(influencerfullName) = 'LESLUNES NL FR' THEN 'NL_FR'
     WHEN UPPER(influencerfullName)='FACEBOOK DE' THEN 'FB'
     WHEN UPPER(influencerfullName)='FACEBOOK FR' THEN 'FB_FR'
     WHEN UPPER(influencerfullName)='FACEBOOK IT' THEN 'FB_IT'
     WHEN influencerfullName='Google Ads DE' THEN 'GOOGLE_ADS'
     WHEN influencerfullName IS NULL THEN 'UNKNOWN' 
     ELSE 'SMM'
     END AS code_type
FROM `leslunes-prep.dbt_coupons.unlooped_all` 
GROUP BY code, influencerfullName 

UNION ALL
SELECT csv_code,wk_code, "WUNDERKIND"
FROM `leslunes-raw.unlooped.wunderkind_codes` 
GROUP BY 1,2,3
