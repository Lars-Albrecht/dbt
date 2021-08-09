SELECT 
    PARSE_DATE("%Y%m%d", date) AS date,
    medium,
    source,	
    users,	
    sessions,	
    percentNewSessions,
    pageviews,
    pageviewPerSession,
    avgSessionDuration,	
    bounceRate
FROM
    `leslunes-raw.ga_kpi_metrics.mkt_channel_performance_fr`
