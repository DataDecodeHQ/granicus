-- granicus:
--   depends_on: [clean_api_data]
--   time_column: event_ts
--   interval_unit: day
--   layer: staging

SELECT
    event_id,
    event_type,
    TIMESTAMP(event_ts) AS event_ts,
    user_id
FROM `my-gcp-project.raw.api_events`
WHERE event_ts IS NOT NULL
