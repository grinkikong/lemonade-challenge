WITH raw_events AS (
    SELECT
        TIMESTAMP,
        EVENT_PAYLOAD:eventId::VARCHAR AS event_id,
        EVENT_PAYLOAD:eventName::VARCHAR AS event_name,
        parse_json(EVENT_PAYLOAD:context) AS context,
        parse_json(EVENT_PAYLOAD:payload) AS payload
    FROM
        {target_db}.spark_emr.raw_events
    WHERE
        TIMESTAMP >= '{date_from}'
        AND TIMESTAMP < '{date_to}'
),
enriched_events AS (
    SELECT
        event_id,
        created_at
    FROM {target_db}.spark_emr.enriched_events
    WHERE
        CREATED_AT >= '{date_from}'
        AND CREATED_AT < '{date_to}'
),
not_in_enriched AS (
    SELECT
        r.event_id,
        r.event_name,
        r.timestamp,
        r.context,
        r.payload
    FROM raw_events r
    LEFT JOIN enriched_events e ON r.event_id = e.event_id
    WHERE e.event_id IS NULL
)
SELECT
    event_id as eventId,
    event_name as eventName,
    DATE_PART('epoch_millisecond', TIMESTAMP)::BIGINT as timestamp,
    context,
    payload
FROM not_in_enriched
WHERE event_name != 'heartbeat'
