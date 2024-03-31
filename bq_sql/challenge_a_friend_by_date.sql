WITH 
    treatment_users AS (
        SELECT 
            user_pseudo_id
            , MIN(event_timestamp_micros) AS intervention_timestamp
        FROM `msds-ut-austin.product_analytics.flood_it_ga4_analytics`
        WHERE event_name = 'challenge_a_friend'
        GROUP BY 1
    )
    , control_users AS (
        SELECT DISTINCT user_pseudo_id
        FROM `msds-ut-austin.product_analytics.flood_it_ga4_analytics`
        WHERE event_name != 'challenge_a_friend'
            AND user_pseudo_id NOT IN (SELECT user_pseudo_id FROM treatment_users)
            AND rand() < 0.001 -- RANDOMLY SAMPLE 0.1% of users
    )
    , users AS (
        SELECT *
            , null AS intervention_timestamp
            , 'control' AS cohort
        FROM control_users
        UNION ALL
        SELECT *
            , 'treatment' AS cohort
        FROM treatment_users
    )
    , timeseries AS (
        SELECT 
            u.*
            , e.event_timestamp_micros AS event_timestamp
            , e.event_name
            , REPLACE(e.event_name, '_quickplay', '') AS cleaned_event_name
            , CASE 
                WHEN e.event_timestamp_micros < intervention_timestamp THEN 'pre-period'
                WHEN e.event_timestamp_micros >= intervention_timestamp THEN 'post-period'
                ELSE 'control-period'
            END AS period_cohort
            , ARRAY_TO_STRING(ARRAY((SELECT key FROM UNNEST(user_properties) WHERE key LIKE 'firebase_exp_%')), ', ') AS exp_nos
            , ARRAY_TO_STRING(ARRAY((SELECT value.string_value FROM UNNEST(user_properties) WHERE key LIKE 'firebase_exp_%')), ', ') AS variant_nos
        FROM users u 
        JOIN `msds-ut-austin.product_analytics.flood_it_ga4_analytics` e
            ON u.user_pseudo_id = e.user_pseudo_id
                AND ( 
                    e.event_name LIKE 'level_start%' -- includes 'level_start_quickplay'
                    OR e.event_name = 'session_start'
                )
        ORDER BY cohort, user_pseudo_id, event_timestamp
    )
    , pre_post AS (
        SELECT 
            cohort
            , period_cohort
            , user_pseudo_id
            , COUNT(CASE WHEN cleaned_event_name = 'level_start' THEN event_timestamp END) AS level_starts
            , COUNT(DISTINCT DATE(CASE WHEN cleaned_event_name = 'level_start' THEN event_timestamp END)) AS days
            , SAFE_DIVIDE( COUNT(CASE WHEN cleaned_event_name = 'level_start' THEN event_timestamp END) , COUNT(DISTINCT DATE(CASE WHEN cleaned_event_name = 'level_start' THEN event_timestamp END)) ) AS level_starts_per_day
            , COUNT(CASE WHEN cleaned_event_name = 'session_start' THEN event_timestamp END) AS sessions
            , SAFE_DIVIDE ( COUNT(CASE WHEN cleaned_event_name = 'session_start' THEN event_timestamp END), COUNT(DISTINCT DATE(CASE WHEN cleaned_event_name = 'session_start' THEN event_timestamp END)) ) AS sessions_per_day
        FROM timeseries
        GROUP BY 1, 2, 3
        QUALIFY STRING_AGG(period_cohort) OVER (PARTITION BY user_pseudo_id) IN ('post-period,pre-period', 'pre-period,post-period', 'control-period')
    )

SELECT *
FROM timeseries
;