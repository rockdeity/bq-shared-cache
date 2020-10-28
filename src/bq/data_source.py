# abstraction data interface for BQ queries
from datetime import datetime, timedelta
from google.cloud import bigquery
import time

from src.source import ParsedSource

yesterday = datetime.now() - timedelta(days=1, hours=7)

yesterdays_date = yesterday.strftime("%Y-%m-%d")
print(yesterdays_date)

#client = bigquery.Client()

class DataSource:

    def __init__(self, source: ParsedSource):
        self._source = source





def get_df(client, project, dataset, table, day):
    table_standard_sql = f'{project}.{dataset}.{table}'
    sql = f"""
        select * from `{table_standard_sql}` where day = "{day}"
    """
    query_config = bigquery.QueryJobConfig()

    tic = time.perf_counter()
    df = client.query(sql, job_config=query_config).to_dataframe()
    toc = time.perf_counter()
