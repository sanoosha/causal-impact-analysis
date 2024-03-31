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
            u.user_pseudo_id
            , u.intervention_timestamp
            , u.cohort
            , e.event_timestamp_micros AS event_timestamp
            , REPLACE(e.event_name, '_quickplay', '') AS event_name
            , CASE 
                WHEN e.event_timestamp_micros < intervention_timestamp THEN 'pre-period'
                WHEN e.event_timestamp_micros >= intervention_timestamp THEN 'post-period'
                ELSE 'control-period'
            END AS period_cohort
            , ARRAY_TO_STRING(ARRAY((SELECT key FROM UNNEST(user_properties) WHERE key LIKE 'firebase_exp_%')), ', ') AS exp_nos
            , ARRAY_TO_STRING(ARRAY((SELECT value.string_value FROM UNNEST(user_properties) WHERE key LIKE 'firebase_exp_%')), ', ') AS variant_nos
            , MIN(e.event_timestamp_micros) OVER (PARTITION BY u.user_pseudo_id, REPLACE(e.event_name, '_quickplay', '')) AS first_event_timestamp
        FROM users u 
        JOIN `msds-ut-austin.product_analytics.flood_it_ga4_analytics` e
            ON u.user_pseudo_id = e.user_pseudo_id
                AND ( 
                    e.event_name LIKE 'level_start%' -- includes 'level_start_quickplay'
                    -- OR e.event_name = 'session_start'
                )
    )

SELECT *
    , CASE WHEN cohort = 'treatment' 
        THEN TIMESTAMP_DIFF(event_timestamp, intervention_timestamp, day)
        ELSE TIMESTAMP_DIFF(event_timestamp, first_event_timestamp, day)
    END AS day
    , CASE WHEN cohort = 'treatment' 
        THEN FLOOR(TIMESTAMP_DIFF(event_timestamp, intervention_timestamp, day)/7)
        ELSE FLOOR(TIMESTAMP_DIFF(event_timestamp, first_event_timestamp, day)/7)
    END AS week
FROM timeseries
ORDER BY user_pseudo_id, event_timestamp