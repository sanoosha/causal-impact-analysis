CREATE TABLE msds-ut-austin.product_analytics.flood_it_ga4_analytics
PARTITION BY partition_date
CLUSTER BY event_name
AS
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  DATE_ADD(PARSE_DATE('%Y%m%d', event_date), interval 6 year) AS partition_date,
  event_timestamp,
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp_micros,
  event_name,
  event_params,
  event_previous_timestamp,
  event_value_in_usd,
  event_bundle_sequence_id,
  event_server_timestamp_offset,
  user_id,
  user_pseudo_id,
  user_properties,
  user_first_touch_timestamp,
  user_ltv,
  device,
  geo,
  app_info,
  traffic_source,
  stream_id,
  platform,
  event_dimensions
FROM
  `firebase-public-project.analytics_153293282.events_*`;
