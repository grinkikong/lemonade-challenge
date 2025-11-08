WITH games AS (
    SELECT game_id
    FROM DATA_WAREHOUSE.L2_ENTITIES.L2__GAMES
    WHERE game_id IN (
        '2dd66893-a64b-44c7-8a82-e3d96e5bdf6e',
        '451e011e-0be8-44b6-8998-c3269955b0fd',
        '451e011e-0be8-44b6-8998-c3269955b0fd',
        '3ff22a7e-0352-4edb-a8b0-841fced1716e',
        '9528b4e1-6583-43a2-9121-82567c2a2a4f',
        '735a1908-0780-4eea-906b-9f8f870103d0',
        '19f46bb9-2447-46f0-b7b6-1b45cf18928f',
        'ff37fd4b-2123-4f1a-a64a-f5f7a18dde16'
    )
),

runs AS (
    SELECT ludeo_id,
           COUNT(*) total_runs,
           SUM(CASE WHEN is_win = TRUE THEN 1 ELSE 0 END) total_wins,
           AVG(score) avg_score, MAX(score) max_score,
           total_wins/total_runs win_rate,
           COUNT(DISTINCT user_id) unique_users,
           SUM(run_duration_net) total_duration,
           SUM(CASE WHEN is_restarted = TRUE THEN 1 ELSE 0 END) total_restarts,
           total_restarts/total_runs restart_rate,
    FROM DATA_WAREHOUSE.L2_ENTITIES.L2__LUDEO_RUNS
    WHERE game_id IN (SELECT game_id FROM games)
    GROUP BY 1
),

drafts_events AS (
    SELECT game_id, ludeo_id, list_of_events_counts
    FROM SANDBOX.PLAYGROUND.SM_DRAFT_EVENTS_PER_LUDEO
    GROUP BY 1,2,3
)

SELECT l.ludeo_id, l.ludeo_name,
       DATE(l.created_at) ludeo_created_at_date,
       l.objective_dynamic_name, l.game_name,
       l.scoring_parameters,
       ludeo_image, ludeo_video,
       d.list_of_events_counts drafts_events,
       lr.total_runs,
       total_wins, win_rate,
       DIV0(SUM(lr.total_duration), lr.total_runs) avg_run_duration,
       avg_score, max_score
FROM DATA_WAREHOUSE.L2_ENTITIES.L2__LUDEOS l
LEFT JOIN runs lr ON l.ludeo_id = lr.ludeo_id
LEFT JOIN drafts_events d ON l.ludeo_id = d.ludeo_id
WHERE l.game_id IN (SELECT game_id FROM games)
  AND l.status = 'active'
  AND l.is_test = FALSE
  AND total_runs > 0
GROUP BY ALL
