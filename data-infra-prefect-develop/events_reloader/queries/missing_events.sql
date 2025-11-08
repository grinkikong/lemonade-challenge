WITH dead_letter_events AS (
    SELECT
        PARSE_JSON(event_payload['original_payload'])['eventId']::VARCHAR AS event_id,
        PARSE_JSON(event_payload['original_payload'])['timestamp']::BIGINT AS timestamp,
        PARSE_JSON(event_payload['original_payload'])['eventName']::VARCHAR AS event_name,
        PARSE_JSON(event_payload['original_payload'])['context']::OBJECT AS context,
        PARSE_JSON(event_payload['original_payload'])['payload']::OBJECT AS original_payload,
        PARSE_JSON(event_payload['original_payload'])::OBJECT AS payload,
        PARSE_JSON(event_payload['original_payload'])['kafka_timestamp_source']::BIGINT AS kafka_timestamp_source
    FROM
        {target_db}.spark_emr.system_events
    WHERE
        event_name = 'dead_letter_queue'
        AND created_at > DATEADD(HOUR, -{hours_back}, CURRENT_TIMESTAMP)
        AND event_payload['service'] like '%eventflow.events-collector.raw-events%'
        AND STARTSWITH(event_payload['error_message'], 'send_post_request failed with status_code')
        AND PARSE_JSON(event_payload['original_payload'])['eventId'] IS NOT NULL
),

enriched_events AS (
    SELECT
        event_id
    FROM
        {target_db}.spark_emr.enriched_events
    WHERE
        created_at > DATEADD(HOUR, -{hours_back}, CURRENT_TIMESTAMP)
)

SELECT
    dle.event_id as eventId,
    dle.event_name as eventName,
    dle.timestamp as timestamp,
    dle.context as context,
    dle.payload as payload,
    dle.original_payload as original_payload
FROM
    dead_letter_events dle
    LEFT JOIN enriched_events e ON dle.event_id = e.event_id
WHERE
    e.event_id IS NULL