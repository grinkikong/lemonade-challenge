SELECT lp.ludeo_id,
       lp.ludeo_name,
       lp.created_at,
       lp.status,
       lp.game_id,
       lp.game_name,
       lp.ludeo_age,
       lp.duration_sec,
       lp.creator_username,
       lp.score_params,
       lp.thumb_image,
       lp.video_link,
       meta.thumbnail_img_analysis,
       meta.video_analysis,
       meta.tags,
       meta.objective_score_params_analysis,
       meta.environment_analysis,
       -- METRICS
       lp.avg_win_score,
       lp.avg_lose_score,
       lp.total_runs,
       lp.total_run_duration_sec,
       lp.avg_run_duration_sec,
       lp.distinct_players,
       lp.total_wins,
       lp.win_ratio,
       lp.total_restarts
FROM DATA_WAREHOUSE.L3_MAIN.L3__LUDEOS_PROFILE lp
LEFT JOIN DATA_WAREHOUSE.SEEDS.GAMES_FOR_REPORTS g ON lp.game_id = g.game_id
LEFT JOIN OPERATIONS.ANALYTICS_OPERATIONS.LUDEOS_METADATA_ANALYSIS meta ON lp.ludeo_id = meta.ludeo_id
WHERE lp.status = 'published'
  AND g.is_relevant = TRUE
  AND lp.ludeo_name NOT ILIKE '%test%'
  AND total_runs > 1
  AND creator_username <> 'Rafael Asor' -- generate by GAME-CHANGER AI ludeos
QUALIFY ROW_NUMBER() OVER(PARTITION BY lp.ludeo_id ORDER BY lp.created_at DESC) = 1
{limit}
