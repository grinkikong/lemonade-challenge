WITH relevant_games AS (
    SELECT DISTINCT game_id
    FROM DATA_WAREHOUSE.SEEDS.GAMES_FOR_REPORTS
    WHERE {games_for_reports_filter}
),

existing_ludeos AS (
    SELECT ludeo_id
    FROM {snowflake_read_operations_db}.ANALYTICS_OPERATIONS.LUDEOS_METADATA_ANALYSIS
    GROUP BY 1
),

ranked_ludeos AS (
    SELECT
        l.ludeo_id,
        l.ludeo_name,
        l.game_name,
        l.thumb_image,
        l.webm_video_link,
        l.draft_url,
        l.objective,
        l.score_params,
        l.total_runs,
        ROW_NUMBER() OVER (PARTITION BY l.game_id ORDER BY l.total_runs DESC) AS ludeo_rank
    FROM {snowflake_read_db}.L3_MAIN.L3__LUDEOS_PROFILE l
    JOIN relevant_games rg ON l.game_id = rg.game_id
    WHERE l.total_runs > 10
)

SELECT *
FROM ranked_ludeos
WHERE ludeo_id NOT IN (SELECT * FROM existing_ludeos)
