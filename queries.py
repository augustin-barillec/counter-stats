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
    kills as (select * from `{project_id}.{dataset_name}.suicides`)

    select * from suicides 
    where  starts_with(player_id, 'STEAM')
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'h_suicides')


def select_map_numbers(gpl):
    query_template = """
    with 
    hh_kills as (select * from `{project_id}.{dataset_name}.hh_kills`)
    
    select distinct map_number from hh_kills
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'selected_map_numbers')


def select_pretty_logs(gpl):
    query_template = """
    with 
    selected_map_numbers as (
    select * from `{project_id}.{dataset_name}.selected_map_numbers`),
    pretty_logs as (select * from `{project_id}.{dataset_name}.pretty_logs`)
    
    select * from pretty_logs 
    where map_number in (select map_number from selected_map_numbers)
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'selected_pretty_logs')


def elect_human_player_names(gpl):
    query_template = """
    with 
    hh_kills as (select * from `{project_id}.{dataset_id}.hh_kills`),

    killers as (
    select 
    killer_id,
    killer_name,
    count(*) as nb_of_kills
    from hh_kills 
    group by killer_id, killer_name), 

    killed as (
    select 
    killed_id,
    killed_name,
    count(*) as nb_of_deaths
    from hh_kills 
    group by killed_id, killed_name),

    players as (
    select 
    ifnull(killer_id, killed_id) as player_id,
    ifnull(killer_name, killed_name) as player_name,
    ifnull(nb_of_kills, 0) as nb_of_kills,
    ifnull(nb_of_deaths, 0) as nb_of_deaths

    from killers full outer join killed 
    on killers.killer_id = killed.killed_id
    and killers.killer_name=killed.killed_name),

    players_1 as (
    select *, nb_of_kills + nb_of_deaths as activity
    from players),

    players_2 as (
    select *, 
    row_number() over(partition by player_id order by activity desc) as rn
    from players_1),

    players_3 as (select player_id, player_name from players_2 where rn=1)

    select * from players_3
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'players')


def reattribute_hh_kills(gpl):
    query_template = """
    with 
    hh_kills as (select * from `{project_id}.{dataset_name}.hh_kills`),
    players as (select * from `{project_id}.{dataset_name}.players`),
    
    hh_kills_1 as (
    select *,
    a.player_name as reattributed_killer_name,
    b.player_name as reattributed_killed_name,
    from kill_logs 
    inner join players a on kill_logs.killer_id = a.player_id
    inner join players b on kill_logs.killed_id = b.player_id)
    
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
    select *,
    players.player_name as reattributed_player_name,
    from kill_logs 
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


def only_friday_evenings(gpl):
    where_filter = """
    where 
    extract(dayofweek from ts) = 6 
    
    and
    
    (extract(hour from ts) >= 22 or 
    (extract(hour from ts) = 21 and extract(minute from ts) >= 30))
    """


def build_stats(gpl):
    query_template = """
    with 
    hh_kills as (select * from `{project_id}.{dataset_id}.hh_kills`),
    h_suicides as (select * from `{project_id}.{dataset_id}.h_suicides`)
    
    
    nb_of_kills as (select d, killer_name, count(*) as kills from hh_kills 
    group by killer_name,d),
    
    nb_of_killed as (select d, killed_name, count(*) as killed from hh_kills 
    group by killed_name,d),
    
    nb_of_suicides as (select d, player_name, count(*) as suicide 
    from h_suicides group by player_name, d),
    
    stats as (select
    ifnull(nb_of_kills.d, nb_of_deaths.d) as d,
    ifnull(killer_name, killed_name) as player_name, 
    ifnull(kills, 0) as kills,
    ifnull(killed, 0) as killed,
    from nb_of_kills full outer join nb_of_killed on
    nb_of_kills.killer_name = nb_of_deaths.killed_name 
    and nb_of_kills.d = nb_of_deaths.d),

    stats_1 as (
    select d, player_name, 
    ifnull(kills, 0) as kills, 
    ifnull(killed, 0) as killed,
    from stats full outer join nb_of_suicides 
    using(player_name, d))
    
    select * from stats_1
    
    select * from stats
    """
    query = format_query(query_template)
    query_to_bq(gpl, query, 'stats_1')
