from resources import project_id, dataset_name


def delete_table(bq_client, table_name):
    bq_client.delete_table(
        '{}.{}.{}'.format(project_id, dataset_name, table_name),
        not_found_ok=True)


def list_tables(bq_client):
    tables = list(
        bq_client.list_tables('{}.{}'.format(project_id, dataset_name))
    )
    return tables


def delete_tables(bq_client, logger):
    tables = list_tables(bq_client)

    for t in tables:
        table_name = t.table_id
        delete_table(bq_client, table_name)
        logger.info('{} deleted'.format(table_name))
