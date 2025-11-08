WITH
    l2_ludeo_plays AS (SELECT * FROM {database}.L2_FACTS.L2__LUDEO_PLAYS),
    l2_ludeos AS (SELECT * FROM {database}.L2_ENTITIES.L2__LUDEOS),
    l2_games AS (SELECT * FROM {database}.L2_ENTITIES.L2__GAMES),
    games_for_reports AS (SELECT * FROM DATA_WAREHOUSE.SEEDS.GAMES_FOR_REPORTS),

ludeo_plays AS (
    SELECT
        game_id,
        ludeo_id,
        count(distinct run_id) runs,
        count(distinct lp.user_id) distinct_players,
        sum(play_duration_sec)/60 run_duration_minutes,
        (sum(play_duration_sec)/60) / count(distinct lp.user_id) time_per_player
    FROM l2_ludeo_plays lp
    GROUP BY game_id, ludeo_id
),

ludeo_stats AS (
    SELECT
        l.game_id,
        lp.ludeo_id,
        l.duration_ms/1000 duration,
        runs,
        distinct_players,
        cast(time_per_player AS FLOAT) time_per_player,
        rank() over (partition by lp.game_id order by time_per_player desc) rnk_most_popular,
        row_number() over (partition by lp.game_id order by random()) rnk_random
    FROM ludeo_plays lp
    JOIN l2_ludeos l ON lp.ludeo_id = l.ludeo_id
    WHERE status = 'published'
      AND is_test = FALSE
      AND duration >= 10
      AND lp.game_id IS NOT NULL
      AND runs > 8
)

SELECT
    ls.*,
    g.game_name,
    current_timestamp() as query_run_at
FROM ludeo_stats ls
LEFT JOIN l2_games g ON ls.game_id = g.game_id
LEFT JOIN games_for_reports gr ON ls.game_id = gr.game_id
WHERE {game_for_reports_filter}
