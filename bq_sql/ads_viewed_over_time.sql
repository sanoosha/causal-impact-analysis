SELECT 
    DATE(DATE_TRUNC(event_timestamp_micros, DAY)) AS reporting_date
    -- DATE(DATE_TRUNC(event_timestamp_micros, WEEK)) AS reporting_week
    , event_name
    -- , platform
    -- , (SELECT value.string_value FROM UNNEST(event_params) WHERE key='firebase_screen_class') AS screen_class

    , COUNT(DISTINCT user_pseudo_id) AS users
    , COUNT(event_timestamp) AS events
    , STRING_AGG(DISTINCT platform) AS platforms
    , STRING_AGG(DISTINCT (SELECT value.string_value FROM UNNEST(event_params) WHERE key='firebase_screen_class')) AS screen_classes
FROM `msds-ut-austin.product_analytics.flood_it_ga4_analytics`
WHERE (
        event_name = 'screen_view'
        AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key='firebase_screen_class') LIKE '%Ad%'
    )
    OR event_name = 'ad_reward'
GROUP BY 1, 2
ORDER BY 1, 2

