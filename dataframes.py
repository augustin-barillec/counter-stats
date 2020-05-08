import pandas
from cols import build_cols
from kinds import kinds


def build_dataframes(data):
    cols = build_cols()

    dfs = dict()
    for kind in kinds:
        dfs[kind] = pandas.DataFrame(data=data[kind], columns=cols[kind])
        dfs[kind].sort_values(by='rn', inplace=True)

    for kind in ['amx_statement']:
        dfs[kind] = dfs[kind].loc[
            dfs[kind]['player_id'].apply(lambda x: x.startswith('STEAM'))]

    for kind in ['amx_statement']:
        dfs[kind] = dfs[kind].loc[
            dfs[kind]['player_id'].apply(
                lambda x: x.startswith('STEAM') or x == 'BOT'
            )
        ]

    for kind in ['kill', 'suicide', 'player_trigger', 'team_trigger']:
        col = None
        if kind == 'kill':
            col = 'killer_team'
        if kind in ('suicide', 'player_trigger'):
            col = 'player_team'
        if kind in 'team_trigger':
            col = 'winner_team'
        dfs[kind] = dfs[kind].loc[
            dfs[kind][col].apply(
                lambda x: x in ('CT', 'T')
            )
        ]

    return dfs
