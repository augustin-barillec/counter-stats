import logging
import queries
from google_pandas_load import LoaderQuickSetup
from delete_tables import delete_tables
from pile_up import pile_up
from sharpen import sharpen
from categorize import categorize
from parse_logs import parse_logs
from dataframes import build_dataframes
from prettify import prettify
from upload import upload_pretty_logs, upload_dfs
from resources import project_id, dataset_name, bucket_name, logs_folder_path

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level='INFO')
logger = logging.getLogger()

gpl = LoaderQuickSetup(
    project_id=project_id,
    dataset_id=dataset_name,
    bucket_name=bucket_name,
    local_dir_path='/tmp',
    separator='|',
    logger=logger)


logger.info('Starting pile_up logs...')
logs = pile_up(logs_folder_path)
logger.info('Ended pile_up logs')

logger.info('Starting sharpen logs...')
logs = sharpen(logs)
logger.info('Ended sharpen logs')

logger.info('Starting categorize logs...')
raw_data = categorize(logs)
logger.info('Ended categorize logs')

logger.info('Starting parse logs...')
data = parse_logs(raw_data)
logger.info('Ended parse logs')

logger.info('Starting build dataframes...')
dfs = build_dataframes(data)
logger.info('Ended build dataframes')

logger.info('Starting prettify logs...')
pretty_logs_df = prettify(dfs)
logger.info('Ended prettigy logs')

logger.info('Starting delete_tables...')
delete_tables(gpl.bq_client, logger)
logger.info('Ended delete_tables')

logger.info('Starting upload dfs...')
upload_dfs(gpl, dfs)
logger.info('Ended upload dfs')

logger.info('Starting upload pretty_logs...')
upload_pretty_logs(gpl, pretty_logs_df)
logger.info('Ended upload pretty_logs')

query_to_bq_steps = [
    'build_hh_kills',
    'build_h_suicides',
    'filter_h_suicides',

    'elect_h_player_names',
    'reattribute_hh_kills',
    'reattribute_h_suicides',

    'compute_tk',

    'compute_map_durations',
    'compute_map_outcomes',
    'select_maps',

    'select_logs',

    'compute_competition',

    'compute_kd_stats',

    'attribute_teams',
    'compute_vd_stats',

    'compute_sequences',
    'add_map_infos_to_sequences',
    'extract_kill_sequences',
    'extract_killed_sequences'
]


for step in query_to_bq_steps:
    logger.info('Starting {}...'.format(step))
    getattr(queries, step)(gpl)
    logger.info('Ended {}'.format(step))
