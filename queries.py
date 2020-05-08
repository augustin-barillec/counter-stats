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
    kills as (select * from `{project_id}.{dataset_name}.kills`)
    
    select * from kills 
    where 
    starts_with(killer_id, 'STEAM') and
    starts_with(killed_id, 'STEAM')
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'hh_kills')


def build_h_suicides(gpl):
    query_template = """
    with 
    suicides as (select * from `{project_id}.{dataset_name}.suicides`)

    select * from suicides 
    where  starts_with(player_id, 'STEAM')
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


def filter_h_suicides(gpl):
    query_template = """
    with 
    h_suicides as (select * from `{project_id}.{dataset_name}.h_suicides`)

    select * from h_suicides
    where method not in ('world')
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'h_suicides')


def build_where_competition():
    where_competition = """
    where 
    extract(dayofweek from ts) = 6 
    
    and
    
    (
    
    extract(hour from ts) >= 22 
    
    or 
    
    (extract(hour from ts) = 21 and  extract(minute from ts) >= 30)
    )
    """
    return where_competition


def build_competition_hh_kills(gpl):
    query_template = """
    select * from `{project_id}.{dataset_name}.hh_kills`
    """
    query = format_query(query_template)
    where_competition = build_where_competition()
    query += where_competition
    query_to_bq(gpl, query, 'competition_hh_kills')


def build_competition_h_suicides(gpl):
    query_template = """
    select * from `{project_id}.{dataset_name}.h_suicides`
    """
    query = format_query(query_template)
    where_competition = build_where_competition()
    query += where_competition
    query_to_bq(gpl, query, 'competition_h_suicides')


def build_individiual_stats(gpl):
    query_template = """
    with 
    competition_hh_kills as (
    select * from `{project_id}.{dataset_name}.competition_hh_kills`),
    comptetition_h_suicides as (
    select * from `{project_id}.{dataset_name}.competition_h_suicides`),
    
    
    nb_of_kills as (select d, killer_name, count(*) as kills 
    from competition_hh_kills 
    group by killer_name,d),
    
    nb_of_killed as (select d, killed_name, count(*) as killed 
    from competition_hh_kills 
    group by killed_name,d),
    
    nb_of_suicides as (select d, player_name, count(*) as suicide 
    from comptetition_h_suicides group by player_name, d),
    
    stats as (select
    ifnull(nb_of_kills.d, nb_of_killed.d) as d,
    ifnull(killer_name, killed_name) as player_name, 
    ifnull(kills, 0) as kills,
    ifnull(killed, 0) as killed,
    from nb_of_kills full outer join nb_of_killed on
    nb_of_kills.killer_name = nb_of_killed.killed_name 
    and nb_of_kills.d = nb_of_killed.d),

    stats_1 as (
    select d, player_name, 
    ifnull(kills, 0) as kills, 
    ifnull(killed, 0) as killed,
    from stats full outer join nb_of_suicides 
    using(player_name, d))
    
    select * from stats_1
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'individual_stats')


def build_map_durations(gpl):
    query_template = """
    with
    map_changes as (select * from `{project_id}.{dataset_name}.map_changes`),
    map_changes_1 as (select *, 
    lead(ts) over(order by rn) as end_ts from map_changes),
    map_changes_2 as (select *,
    timestamp_diff(end_ts, ts, MINUTE) as duration
    from map_changes_1 where end_ts is not null)
    select * from map_changes_2
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'map_durations')


def build_map_outcomes(gpl):
    query_template = """
    with 
    team_triggers as (
    select * from `{project_id}.{dataset_name}.team_triggers`),
    team_victories as (
    select 
    map_number,
    max(ct_score) as max_ct_score,
    max(t_score) as max_t_score,
    from team_triggers
    group by map_number)

    select 
    *,
    case 
    when max_ct_score = max_t_score then 'draw'
    when max_ct_score > max_t_score then 'ct_victory'
    when max_t_score > max_ct_score then 't_victory'
    end as outcome
    from team_victories
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'map_outcomes')


def build_map_summaries(gpl):
    query_template = """
    with 
    map_durations as (select * 
    from `{project_id}.{dataset_name}.map_durations`),
    map_outcomes as (select * 
    from `{project_id}.{dataset_name}.map_outcomes`),

    map_summaries as (select *
    from map_durations inner join map_outcomes using (map_number))

    select * from map_summaries
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'map_summaries')


def build_map_teams(gpl):
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
    query_to_bq(gpl, query, 'map_teams')


def select_maps(gpl):
    query_template = """
    with 
    hh_kills as (select * from `{project_id}.{dataset_name}.map_summaries`),
    map_summaries as (select * 
    from `{project_id}.{dataset_name}.map_summaries`)
    
    select * from map_summaries
    where duration > 5 
    and outcome in ('ct_victory', 't_victory')
    and greatest(max_ct_score, max_t_score) > 4
    and max_ct_score <= 15 
    and max_t_score <= 15
    and map_number in (select map_number from hh_kills)
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'selected_maps')


def build_competition_maps(gpl):
    query_template = """
    select * from `{project_id}.{dataset_name}.selected_maps`
    """
    query = format_query(query_template)
    where_competition = build_where_competition()
    query += where_competition
    query_to_bq(gpl, query, 'competition_maps')


def build_team_stats(gpl):
    query_template = """
    with 
    map_teams as (select * 
    from `{project_id}.{dataset_name}.map_teams`),
    competition_maps as (select * 
    from `{project_id}.{dataset_name}.competition_maps`),
    
    stats as (
    select *
    from map_teams inner join competition_maps using (map_number)),
    
    stats_1 as (
    select *, 
    case
    when player_team = 'T' and outcome = 't_victory' then true
    when player_team = 'CT' and outcome = 'ct_victory' then true 
    else False end as is_winner
    from stats),
    
    stats_2 as (
    select 
    player_name, 
    d,
    countif(is_winner) as victories,
    countif(not is_winner) as defeats
    from stats_1
    group by player_name, d)
    
    select * from stats_2
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'team_stats')
