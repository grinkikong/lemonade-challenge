SELECT REPLACE(event_name, '#', '_') as event_name -- parsing event_name from design_system: '#' --> '_'
FROM {database}.SPARK_EMR.ENRICHED_EVENTS
WHERE DATE(created_at) >= CURRENT_DATE-1
  AND (entity = 'client' OR entity IS NULL)
GROUP BY event_name