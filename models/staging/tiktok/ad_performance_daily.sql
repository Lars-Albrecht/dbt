SELECT 
SAFE_CAST((SAFE_CAST(stat_time_day AS DATETIME)) AS DATE) AS stat_time_day, 
ad_id, 
campaign_name, 
campaign_id, 
SAFE_CAST(campaign_budget AS NUMERIC) AS campaign_budget, 
adgroup_name, 
adgroup_id, 
placement_type,
promotion_type, 
SAFE_CAST(budget AS NUMERIC) AS budget, 
ad_name,  
ad_text, 
SAFE_CAST(spend AS NUMERIC) AS spend, 
SAFE_CAST(cpc AS NUMERIC) AS cpc, 
SAFE_CAST(cpm AS NUMERIC) AS cpm, 
SAFE_CAST(impressions AS NUMERIC) AS impressions, 
SAFE_CAST(clicks AS NUMERIC) AS clicks, 
SAFE_CAST(ctr AS NUMERIC) AS ctr,
SAFE_CAST(reach AS NUMERIC) AS reach, 
SAFE_CAST(cost_per_1000_reached AS NUMERIC) AS cost_per_1000_reach, 
SAFE_CAST(conversion AS NUMERIC) AS conversion, 
SAFE_CAST(cost_per_conversion AS NUMERIC) AS cost_per_conversion, 
SAFE_CAST(conversion_rate AS NUMERIC) AS conversion_rate, 
SAFE_CAST(frequency AS NUMERIC) AS frequency, 
SAFE_CAST(video_watched_2s AS NUMERIC) AS video_watched_2s, 
SAFE_CAST(video_watched_6s AS NUMERIC) AS video_watched_6s, 
SAFE_CAST(average_video_play AS NUMERIC) AS average_video_play, 
SAFE_CAST(engaged_view AS NUMERIC) AS engaged_view, 
SAFE_CAST(follows AS NUMERIC) AS follows, 
data_extraction_timestamp FROM (
  SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY stat_time_day, ad_id ORDER BY data_extraction_timestamp DESC) as rn
  FROM 
  `leslunes-raw.tiktok_ads.ad_performance_report`
) ad_test
WHERE ad_test.rn = 1
ORDER BY data_extraction_timestamp DESC