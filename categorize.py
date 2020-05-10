from parse_log import log_to_map
from kinds import kinds


def is_say_log(log):
    return 'say' in log


def is_amx_statement_log(log):
    return 'triggered "amx_say"' in log


def is_chat_statement_log(log):
    return (
            is_say_log(log)
            and not is_amx_statement_log(log)
            and ('BOT' in log or 'STEAM' in log)
    )


def is_kill_log(log):
    return not is_say_log(log) and 'killed' in log


def is_suicide_log(log):
    return not is_say_log(log) and 'suicide' in log


def is_player_trigger_log(log):
    return (
            not is_say_log(log)
            and 'triggered' in log
            and ('BOT' in log or 'STEAM' in log)
    )


def is_team_trigger_log(log):
    return (
            not is_say_log(log)
            and 'triggered' in log
            and 'Team' in log
    )


def is_map_change_log(log):
    return not is_say_log(log) and 'Mapchange' in log


def is_round_start_log(log):
    return (not is_say_log(log)
            and 'World triggered "Round_Start"' in log)


def categorize(logs):
    raw_data = dict()
    for kind in kinds:
        raw_data[kind] = []

    current_map = None
    current_map_number = 0
    current_round_number = 0
    for i, log in enumerate(logs, 1):
        row = {'rn': i, 'log': log}

        is_log_added = False
        for kind in kinds:

            if globals()['is_{}_log'.format(kind)](log):
                if kind == 'map_change':
                    current_map = log_to_map(log)
                    current_map_number += 1
                    current_round_number = 0
                if kind == 'round_start':
                    current_round_number += 1
                row['map'] = current_map
                row['map_number'] = current_map_number
                row['round_number'] = current_round_number
                row['kind'] = kind
                raw_data[kind].append(row)
                is_log_added = True
                break

        if not is_log_added:
            raise (ValueError(log))

    return raw_data
