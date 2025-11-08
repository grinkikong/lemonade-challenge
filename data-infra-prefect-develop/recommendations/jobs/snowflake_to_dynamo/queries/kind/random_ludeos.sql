-- Random ludeos query 
WITH base_data AS (
{base_query}
)

SELECT 
    game_id,
    game_name,
    LISTAGG(ludeo_id, ',') WITHIN GROUP (ORDER BY rnk_random) AS sorted_ludeo_ids,
    query_run_at
FROM base_data
GROUP BY game_id, game_name, query_run_at
LIMIT 200