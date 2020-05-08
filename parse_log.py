from datetime import datetime
from normalize_team_name import normalize_team_name


def log_to_timestamp(log):
    res = log[2:23]
    res = datetime.strptime(res, '%m/%d/%Y - %H:%M:%S')
    return res


def log_to_player_info(log, doer=True):
    if doer:
        indice = 1
    else:
        indice = 3
    aux = log.split('"')[indice]
    aux = aux.split('<')
    player_name = aux[0]
    player_id = aux[2][:-1]
    player_team = aux[3][:-1]
    return player_id, player_name, player_team


def log_to_player_id(log, doer=True):
    return log_to_player_info(log, doer)[0]


def log_to_player_name(log, doer=True):
    return log_to_player_info(log, doer)[1]


def log_to_player_team(log, doer=True):
    return normalize_team_name(log_to_player_info(log, doer)[2])


def log_to_winner_team(log):
    return normalize_team_name(log.split('"')[1])


def log_to_loser_team(log):
    winner_team = log_to_winner_team(log)
    if winner_team == 'T':
        return 'CT'
    if winner_team == 'CT':
        return 'T'
    return None


def log_to_ct_score(log):
    return int(log.split('"')[5])


def log_to_t_score(log):
    return int(log.split('"')[7])


def log_to_win_reason(log):
    return log.split('"')[3]


def before_last_bit(log):
    return log.split('"')[-2]


log_to_kill_weapon = before_last_bit
log_to_suicide_method = before_last_bit
log_to_player_special_action = before_last_bit
log_to_text = before_last_bit


def log_to_map(log):
    res = log.split('--------')[1]
    res = res[14:-1]
    return res


def log_to_is_say_team(log):
    return 'say_team' in log.split('"')[2]


def log_to_is_alive(log):
    return 'dead' not in log.split('"')[-1]
