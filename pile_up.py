import os
from parse_log import log_to_timestamp

forbidden_words = [
    '|',
    'adminchat.amxx',
    'amx_psay',
    'amx_csay',
    'amx_tsay',
    'amx_chat'
]

key_words = [
    'triggered "amx_say"',

    'killed',
    'suicide',

    'Mapchange',
    'World triggered "Round_Start"'
]

key_couples = [
    ('BOT', 'say'),
    ('STEAM', 'say'),
    ('BOT', 'triggered'),
    ('STEAM', 'triggered'),
    ('Team', 'triggered')
]


def is_to_keep_log(log):
    if not log.startswith('L'):
        return False
    for w in forbidden_words:
        if w in log:
            return False
    for w in key_words:
        if w in log:
            return True
    for w1, w2 in key_couples:
        if w1 in log and w2 in log:
            return True
    return False


def pile_up(folder_path):
    logs = []

    previous_max_timestamp = None
    for sub_folder_basename in sorted(os.listdir(folder_path)):
        sub_folder_path = os.path.join(folder_path, sub_folder_basename)
        sub_logs = []

        for file_basename in os.listdir(sub_folder_path):
            file_path = os.path.join(sub_folder_path, file_basename)
            with open(file_path) as f:
                sub_logs += list(f)

        kept_logs = [log for log in sub_logs if is_to_keep_log(log)]

        if previous_max_timestamp is not None:
            kept_logs = [log for log in kept_logs if
                         log_to_timestamp(log) > previous_max_timestamp]

        timestamps = [log_to_timestamp(log) for log in kept_logs]
        max_timestamp = max(timestamps)
        previous_max_timestamp = max_timestamp

        logs += sorted(kept_logs, key=lambda x: log_to_timestamp(x))

    return logs
