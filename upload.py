from kinds import kinds
from google_pandas_load import LoadConfig
from data_names import build_data_names
from cols import build_cols


def upload_pretty_logs(gpl, pretty_logs_df):
    gpl.load(
        source='dataframe',
        destination='bq',
        data_name='raw_pretty_logs',
        dataframe=pretty_logs_df,
        timestamp_cols=['ts'],
        date_cols=['d'])


def build_cols_to_upload():
    cols = build_cols()
    cols_to_upload = dict()
    for kind in kinds:
        cols_to_upload[kind] = [c for c in cols[kind] if c != 'log']
    return cols_to_upload


def upload_dfs(gpl, dfs):
    data_names = build_data_names()
    cols_to_upload = build_cols_to_upload()

    configs = [
        LoadConfig(
            source='dataframe',
            destination='bq',
            data_name=data_names[kind],
            dataframe=dfs[kind][cols_to_upload[kind]],
            timestamp_cols=['ts'],
            date_cols=['d'])
        for kind in kinds]

    gpl.mload(configs)
