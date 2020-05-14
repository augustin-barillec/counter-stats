from query_to_bq import query_to_bq
from resources import project_id, dataset_name


def format_query(query_template):
    query = query_template.format(
        project_id=project_id,
        dataset_name=dataset_name)
    return query


def build_hh_kills(gpl):
    query_template = """
    with 
    raw_kills as (select * from `{project_id}.{dataset_name}.raw_kills`)
    
    select * from raw_kills 
    where 
    starts_with(killer_id, 'STEAM') and
    starts_with(killed_id, 'STEAM')
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'hh_kills')


def build_h_suicides(gpl):
    query_template = """
    with 
    raw_suicides as (select * from `{project_id}.{dataset_name}.raw_suicides`)

    select * from raw_suicides 
    where  starts_with(player_id, 'STEAM')
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'h_suicides')


def filter_h_suicides(gpl):
    query_template = """
    with
    h_suicides as (select * from `{project_id}.{dataset_name}.h_suicides`)

    select * from h_suicides
    where method not in ('world')
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'h_suicides')


def elect_h_player_names(gpl):
    query_template = """
    with 
    hh_kills as (select * from `{project_id}.{dataset_name}.hh_kills`),

    players as (
    select 
    killer_id as player_id,
    killer_name as player_name,
    count(*) as nb_of_kills
    from hh_kills 
    group by killer_id, killer_name), 

    players_1 as (
    select *, 
    row_number() over(partition by player_id order by nb_of_kills desc) as rn
    from players),

    players_2 as (select player_id, player_name from players_1 where rn=1)

    select * from players_2
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'players')


def reattribute_hh_kills(gpl):
    query_template = """
    with 
    hh_kills as (select * from `{project_id}.{dataset_name}.hh_kills`),
    players as (select * from `{project_id}.{dataset_name}.players`),
    
    hh_kills_1 as (
    select hh_kills.*except(killer_name, killed_name),
    a.player_name as killer_name,
    b.player_name as killed_name,
    from hh_kills 
    inner join players a on hh_kills.killer_id = a.player_id
    inner join players b on hh_kills.killed_id = b.player_id)
    
    select * from hh_kills_1
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'hh_kills')


def reattribute_h_suicides(gpl):
    query_template = """
    with 
    h_suicides as (select * from `{project_id}.{dataset_name}.h_suicides`),
    players as (select * from `{project_id}.{dataset_name}.players`),

    h_suicides_1 as (
    select h_suicides.*except(player_name),
    players.player_name as player_name,
    from h_suicides 
    inner join players using(player_id))

    select * from h_suicides_1
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'h_suicides')


def compute_tk(gpl):
    query_template = """
    with 
    hh_kills as (select * from `{project_id}.{dataset_name}.hh_kills`)
    
    select *, 
    killer_team = killed_team as tk
    from hh_kills
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'hh_kills')


def compute_map_durations(gpl):
    query_template = """
    with
    raw_map_changes as (select * 
    from `{project_id}.{dataset_name}.raw_map_changes`),
    
    maps as (select *, 
    lead(ts) over(order by rn) as end_ts from raw_map_changes),
    
    maps_1 as (select *,
    timestamp_diff(end_ts, ts, MINUTE) as duration from maps 
    where end_ts is not null)
    
    select * from maps_1
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'maps')


def compute_map_outcomes(gpl):
    query_template = """
    with 
    maps as (
    select * from `{project_id}.{dataset_name}.maps`),
    raw_team_triggers as (
    select * from `{project_id}.{dataset_name}.raw_team_triggers`),
    
    team_victories as (
    select 
    map_number,
    max(ct_score) as max_ct_score,
    max(t_score) as max_t_score,
    from raw_team_triggers
    group by map_number),

    team_victories_1 as ( 
    select *,
    case 
    when max_ct_score = max_t_score then 'draw'
    when max_ct_score > max_t_score then 'ct_victory'
    when max_t_score > max_ct_score then 't_victory'
    end as outcome
    from team_victories),
    
    maps_1 as (
    select * from maps inner join team_victories_1 using(map_number))
    
    select * from maps_1
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'maps')


def select_maps(gpl):
    query_template = """
    with 
    hh_kills as (select * from `{project_id}.{dataset_name}.hh_kills`),
    h_suicides as (select * from `{project_id}.{dataset_name}.h_suicides`),
    maps as (select * from `{project_id}.{dataset_name}.maps`)

    select * from maps
    where 
    map_number in (select map_number from hh_kills)
    or 
    map_number in (select map_number from h_suicides)
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'maps')


def select_logs(gpl):
    query_template = """
    with 
    pretty_logs as (select * 
    from `{project_id}.{dataset_name}.raw_pretty_logs`),
    maps as (select * from `{project_id}.{dataset_name}.maps`)
    
    select * from pretty_logs 
    where map_number in (select map_number from maps)
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'pretty_logs')


def build_competition_col():
    competition_col = """
    extract(dayofweek from ts) = 6 

    and

    (

    extract(hour from ts) >= 22 

    or 

    (extract(hour from ts) = 21 and  extract(minute from ts) >= 30)
    ) as competition
    """
    return competition_col


def compute_competition(gpl):
    query_template = """
    select *, {competition_col} from `{project_id}.{dataset_name}.{table_name}`
    """
    competition_col = build_competition_col()
    for table_name in ['hh_kills', 'h_suicides', 'maps', 'pretty_logs']:
        query = query_template.format(
            competition_col=competition_col,
            project_id=project_id,
            dataset_name=dataset_name,
            table_name=table_name)
        query_to_bq(gpl, query, table_name)


def compute_kd_stats(gpl):
    query_template = """
    with 
    hh_kills as (
    select * from `{project_id}.{dataset_name}.hh_kills`),
    h_suicides as (
    select * from `{project_id}.{dataset_name}.h_suicides`),

    kills as (
    select 
    rn, ts, h, d, map, map_number, round_number, competition,
    killer_name as player_name,
    killer_team as player_team,
    cast(not tk as int64) as kills,
    cast(tk as int64) as team_kills, 
    0 as killed,
    0 as team_killed,
    0 as suicides
    from hh_kills), 
    
    killed as (
    select 
    rn, ts, h, d, map, map_number, round_number, competition,
    killed_name as player_name,
    killed_team as player_team,
    0 as kills,
    0 as team_kills, 
    cast(not tk as int64) as killed,
    cast(tk as int64) as team_killed,
    0 as suicides
    from hh_kills), 
    
    suicides as (
    select 
    rn, ts, h, d, map, map_number, round_number, competition,
    player_name,
    player_team,
    0 as kills,
    0 as team_kills, 
    0 as killed,
    0 as team_killed,
    1 as suicides
    from h_suicides),  
    
    kd_stats as (
    select * from kills union all
    select * from killed union all
    select * from suicides)
    
    select * from kd_stats
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'kd_stats')


def attribute_teams(gpl):
    query_template = """
    with 
    hh_kills as (
    select * from `{project_id}.{dataset_name}.hh_kills`),

    nb_of_kills as (select 
    map_number, 
    killer_name, 
    countif(killer_team='CT') as kills_as_ct, 
    countif(killer_team='T') as kills_as_t
    from hh_kills
    group by killer_name, map_number),

    nb_of_killed as (select 
    map_number, 
    killed_name, 
    countif(killed_team='CT') as killed_as_ct, 
    countif(killed_team='T') as killed_as_t
    from hh_kills
    group by killed_name, map_number),

    stats as (select
    ifnull(nb_of_kills.map_number, nb_of_killed.map_number) as map_number,
    ifnull(killer_name, killed_name) as player_name, 
    ifnull(kills_as_ct, 0) as kills_as_ct,
    ifnull(kills_as_t, 0) as kills_as_t,
    ifnull(killed_as_ct, 0) as killed_as_ct,
    ifnull(killed_as_t, 0) as killed_as_t,
    from nb_of_kills full outer join nb_of_killed on
    nb_of_kills.killer_name = nb_of_killed.killed_name 
    and nb_of_kills.map_number = nb_of_killed.map_number),

    stats_1 as (select *, 
    kills_as_ct + killed_as_ct as activity_as_ct,
    kills_as_t + killed_as_t as activity_as_t
    from stats),

    stats_2 as (select *, 
    case 
    when activity_as_ct > activity_as_t then 'CT'
    when activity_as_t > activity_as_ct then 'T'
    else null end
    as player_team
    from stats_1)

    select * from stats_2 where player_team is not null
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'team_attributions')


def compute_vd_stats(gpl):
    query_template = """
    with 
    team_attributions as (select * 
    from `{project_id}.{dataset_name}.team_attributions`),
    maps as (select * 
    from `{project_id}.{dataset_name}.maps`),

    stats as (
    select *
    from team_attributions inner join maps using (map_number)),
    
    stats_1 as (
    select * from stats where outcome in ('ct_victory', 't_victory')),

    stats_2 as (
    select *, 
    case
    when player_team = 'CT' and outcome = 'ct_victory' then true
    when player_team = 'T' and outcome = 't_victory' then true 
    else False end as is_winner
    from stats_1)

    select *, 
    cast(is_winner as int64) as victories,
    cast(not is_winner as int64) as defeats
    from stats_2
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'vd_stats')


def compute_sequences(gpl):
    query_template = """
    with
    hh_kills as (select * from `{project_id}.{dataset_name}.hh_kills`),
    hh_no_team_kills as (select * from hh_kills where not tk),
    
    kills as (select *, killer_name as player_name, 1 as type 
    from hh_no_team_kills),
    killed as (select *, killed_name as player_name, -1 as type 
    from hh_no_team_kills),
    
    events as (select * from kills union all select * from killed),
    
    events_1 as (select *,
    type - lag(type) over(partition by player_name order by rn) 
    as step
    from events),
    
    events_2 as (select *, 
    countif(abs(step) > 1) 
    over (partition by player_name order by rn) as nb_of_breaks
    from events_1),
    
    sequences as (
    select 
    player_name, 
    type, 
    count(*) as length, 
    min(rn) as rn,
    min(ts) as ts,		
    min(d) as d,
    min(h) as h,
    min(map) as map,	
    min(map_number) as map_number,		
    min(round_number) as round_number,	
    min(killer_team) as killer_team,		
    min(killed_team) as killed_team,		
    min(weapon) as weapon,	
    min(killer_name) as killer_name,		
    min(killed_name) as killed_name,		
    min(competition) as competition
    from events_2 
    group by player_name, nb_of_breaks, type
    having length >= 3)
            
    select * from sequences
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'sequences')
