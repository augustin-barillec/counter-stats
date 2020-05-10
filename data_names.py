from kinds import kinds


def build_data_names():
    data_names = dict()
    for kind in kinds:
        data_names[kind] = 'raw_' + kind + 's'
    return data_names
