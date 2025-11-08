-- Most popular ludeos across ALL games (no game breakdown)
WITH base_data AS (
    {base_query}
)

SELECT 
    LISTAGG(ludeo_id, ',') WITHIN GROUP (ORDER BY time_per_player DESC) AS sorted_ludeo_ids,
    query_run_at
FROM base_data
GROUP BY query_run_at
LIMIT 1