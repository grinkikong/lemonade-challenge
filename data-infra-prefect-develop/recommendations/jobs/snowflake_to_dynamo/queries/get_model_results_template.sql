SELECT r.game_id, r.game_name, {snowflake_list_column}, model_run_id
FROM {database}.COLD_START.{table} r
LEFT JOIN DATA_WAREHOUSE.SEEDS.GAMES_FOR_REPORTS g ON r.game_id = g.game_id
WHERE g.is_relevant = TRUE
  AND r.game_id <> '11b2b363-8e9d-4e50-bd9a-83502b2a4768'  -- ROBOCOP has too many ludeos
