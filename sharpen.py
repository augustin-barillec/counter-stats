from categorize import is_map_change_log


def first_map_change_index(logs):
    for i, log in enumerate(logs):
        if is_map_change_log(log):
            return i


def last_map_change_index(logs):
    i = first_map_change_index(reversed(logs))
    return len(logs) - 1 - i


def sharpen(logs):
    first = first_map_change_index(logs)
    last = last_map_change_index(logs)
    return logs[first:last+1]
