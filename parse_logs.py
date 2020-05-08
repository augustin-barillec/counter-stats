from copy import deepcopy
from parse_log import *
from kinds import kinds


def parse_logs(raw_data):
    data = dict()
    for kind in kinds:
        data[kind] = []

    for kind in kinds:
        for row in raw_data[kind]:
            row = deepcopy(row)

            log = row['log']

            ts = log_to_timestamp(log)
            d = ts.date()
            row['ts'] = ts
            row['d'] = d

            if kind in {
                'amx_statement',
                'chat_statement',
                'suicide',
                'player_trigger'
            }:
                row['player_id'] = log_to_player_id(log)
                row['player_name'] = log_to_player_name(log)
                if kind != 'amx_statement':
                    row['player_team'] = log_to_player_team(log)

            if kind in {'amx_statement', 'chat_statement'}:
                row['text'] = log_to_text(log)

            if kind == 'chat_statement':
                row['is_alive'] = log_to_is_alive(log)
                row['is_say_team'] = log_to_is_say_team(log)

            if kind == 'kill':
                row['killer_id'] = log_to_player_id(log)
                row['killer_name'] = log_to_player_name(log)
                row['killer_team'] = log_to_player_team(log)

                row['killed_id'] = log_to_player_id(log, False)
                row['killed_name'] = log_to_player_name(log, False)
                row['killed_team'] = log_to_player_team(log, False)

                row['weapon'] = log_to_kill_weapon(log)

            if kind == 'suicide':
                row['method'] = log_to_suicide_method(log)

            if kind == 'player_trigger':
                row['action'] = log_to_player_special_action(log)

            if kind == 'team_trigger':
                row['winner_team'] = log_to_winner_team(log)
                row['loser_team'] = log_to_loser_team(log)
                row['ct_score'] = log_to_ct_score(log)
                row['t_score'] = log_to_t_score(log)
                row['win_reason'] = log_to_win_reason(log)

            data[kind].append(row)

    return data
