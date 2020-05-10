import pandas
from copy import deepcopy
from kinds import kinds


def pretty_amx_statement(row):
    res = '{} amx_say: {}'.format(
        row['player_name'],
        row['text']
    )
    return res


def pretty_chat_statement(row):
    if row['is_say_team']:
        say = 'say_team'
    else:
        say = 'say'
    if row['player_team'] == '':
        player_team = ''
    else:
        player_team = '({})'.format(row['player_team'])
    if row['is_alive']:
        dead = ''
    else:
        dead = '(dead)'

    res = '{}{}{} {}: {}'.format(
        row['player_name'],
        player_team,
        dead,
        say,
        row['text']
    )
    return res


def pretty_kill(row):
    res = '{}({}) killed {}({}) with {}'.format(
        row['killer_name'],
        row['killer_team'],
        row['killed_name'],
        row['killed_team'],
        row['weapon']
    )
    return res


def pretty_suicide(row):
    res = '{}({}) committed suicide with {}'.format(
        row['player_name'],
        row['player_team'],
        row['method'],
    )
    return res


def pretty_player_trigger(row):
    res = '{}({}) triggered {}'.format(
        row['player_name'],
        row['player_team'],
        row['action'],
    )
    return res


def pretty_team_trigger(row):
    res = '{} triggered {} (CT: {}, T: {})'.format(
        row['winner_team'],
        row['win_reason'],
        row['ct_score'],
        row['t_score']
    )
    return res


def pretty_map_change(row):
    res = 'Map change'.format(
        row['map'])
    return res


def pretty_round_start(row):
    return 'Round start'


def prettify(dfs):
    pretty_logs = dict()
    for kind in kinds:
        pretty_logs[kind] = deepcopy(dfs[kind])
        pretty_logs[kind]['pretty_log'] = pretty_logs[kind].apply(
            globals()['pretty_{}'.format(kind)],
            axis=1)

    to_concat = []
    cols = [
        'rn',
        'ts',
        'd',
        'map',
        'map_number',
        'round_number',
        'kind',
        'pretty_log'
    ]

    for kind in kinds:
        to_concat.append(pretty_logs[kind][cols])
    pretty_logs_df = pandas.concat(to_concat)
    return pretty_logs_df
