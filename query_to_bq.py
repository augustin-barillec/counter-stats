def query_to_bq(gpl, query, data_name):
    gpl.load(
        source='query',
        destination='bq',
        data_name=data_name,
        query=query)
