with score_params as (select * from  {data_warehouse_db}.l2_entities.l2__score_params  where ENABLED = 'true' and
game_id = '{game_id}'
qualify row_number () over (partition by game_id, lower(name) order by created_at desc)=1
),

gameplay_events as (
                    select * from {datalake_db}.SPARK_EMR.GAME_EVENTS
                    where
                    ((event_value NOT ILIKE '%true%'
                    AND
                    event_value NOT ILIKE '%false%')
                    or event_value is null)
                    AND (source not ilike '%bot%' or source is null)
                    AND created_at >= '{max_date}'
                    order by timestamp desc
                    limit 500000
                    ),

room_playes as (select * from {data_warehouse_db}.L1_EVENTS_TABLES.L1__ROOM_EVENTS  qualify row_number() over (partition by SERVER_ROOM_ID order by created_at)=1),
game_playes as (select * from {data_warehouse_db}.L2_FACTS.L2__GAMEPLAYS),


gameplay_data as (
select distinct
to_timestamp(g.timestamp)::string as timestamp,
coalesce(rp.game_id,gp.game_id,g.game_id) as game_id,
g.* exclude (timestamp,game_id,object_id, event_value),
TO_DOUBLE(event_value) as event_value,
row_number () over (partition by g.game_id, g.room_id, g.message_type , g.event_key order by timestamp) action_value
from gameplay_events g
left join room_playes rp
on g.room_id = rp.SERVER_ROOM_ID
left join game_playes gp
on g.room_id = gp.GAMEPLAY_ID
)


,final as (
select
g.game_id,
g.timestamp,
g.EVENT_KEY,
TO_DOUBLE(case when g.event_value is null then action_value else g.EVENT_VALUE end) as EVENT_VALUE,
coalesce(g.room_id,g.gameplay_id) AS GAMEPLAY_ID,
case when message_type is null then 'state' else message_type end as MESSAGE_TYPE,
SOURCE,
AGGREGATION_FUNCTION,
FRIENDLY_NAME,
concat(coalesce(FRIENDLY_NAME,'nan'),'-',coalesce(AGGREGATION_FUNCTION,'nan'),'-',g.EVENT_KEY) NAME_AGGFUNC_EVENTKEY,
count(sp.event_key) over (partition by g.game_id) as flag
from gameplay_data g
left join score_params sp
on g.game_id = sp.game_id
and g.event_key = sp.event_key
where g.game_id = '{game_id}'
)

select * exclude (flag) from final
where flag >0