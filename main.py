import logging
from google_pandas_load import LoaderQuickSetup
from pile_up import pile_up
from categorize import categorize
from parse_logs import parse_logs
from dataframes import build_dataframes
from prettify import prettify
from upload import upload_pretty_logs, upload_dfs
# from select_pretty_logs import select_map_numbers, select_pretty_logs
from resources import project_id, dataset_name, bucket_name, logs_folder_path

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level='INFO')
logger = logging.getLogger()

gpl = LoaderQuickSetup(
    project_id=project_id,
    dataset_id=dataset_name,
    bucket_name=bucket_name,
    local_dir_path='/tmp',
    separator='|')

logger.info('Starting pile_up logs...')
logs = pile_up(logs_folder_path)
logger.info('Ended pile_up logs')

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

logger.info('Starting upload dfs...')
upload_dfs(gpl, dfs)
logger.info('Ended upload dfs')

logger.info('Starting upload pretty_logs...')
upload_pretty_logs(gpl, pretty_logs_df)
logger.info('Ended upload pretty_logs')

# logger.info('Starting select pretty_logs...')
# select_map_numbers(gpl)
# select_pretty_logs(gpl)
# logger.info('Ended select pretty_logs')
#
# logger.info('Starting elect player_names...')
#
# logger.info('Ended elect player_names')
