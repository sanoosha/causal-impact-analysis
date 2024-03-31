SELECT 
    DATE(DATE_TRUNC(event_timestamp_micros, DAY)) AS reporting_date
    , CASE WHEN event_name = 'screen_view' THEN 'Ad Viewed'
        ELSE INITCAP(REPLACE(REPLACE(event_name, '_quickplay', ''), '_', ' '))
    END AS event_name
    -- , platform
    -- , app_info.version
    , COUNT(DISTINCT user_pseudo_id) AS users
    , COUNT(event_timestamp) AS events
    , STRING_AGG(DISTINCT app_info.version ORDER BY app_info.version) AS app_version
FROM `msds-ut-austin.product_analytics.flood_it_ga4_analytics`
WHERE (
    event_name = 'session_start'
    OR event_name LIKE 'level_start%'
    OR (
        event_name = 'screen_view'
        AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key='firebase_screen_class') LIKE '%Ad%'
    )
)
GROUP BY 1, 2
ORDER BY 1, 2