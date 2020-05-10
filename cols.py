from kinds import kinds


def build_cols():
    cols = dict()

    for kind in kinds:
        cols[kind] = [
            'log',
            'rn',
            'ts',
            'd',
            'map',
            'map_number',
            'round_number',
            'kind'
        ]

    for kind in [
        'amx_statement',
        'chat_statement',
        'suicide',
        'player_trigger'
    ]:
        cols[kind] += ['player_id', 'player_name']
        if kind != 'amx_statement':
            cols[kind].append('player_team')

    for kind in ['amx_statement', 'chat_statement']:
        cols[kind] += ['text']

    cols['chat_statement'] += ['is_alive', 'is_say_team']

    cols['kill'] += [
        'killer_id',
        'killer_name',
        'killer_team',
        'killed_id',
        'killed_name',
        'killed_team',
        'weapon'
    ]

    cols['suicide'].append('method')
    cols['player_trigger'].append('action')
    cols['team_trigger'] += [
        'winner_team',
        'loser_team',
        'ct_score',
        't_score',
        'win_reason'
    ]

    return cols
