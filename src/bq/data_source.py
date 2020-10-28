# abstraction data interface for BQ queries
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta
from google.cloud import bigquery
import logging
import time
from typing import Tuple, Union, Dict, List, Set

from src.source import EncodedSource

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.INFO)
logger = logging.getLogger(__name__)

yesterday = datetime.now() - timedelta(days=1, hours=7)

yesterdays_date = yesterday.strftime("%Y-%m-%d")
print(yesterdays_date)

#client = bigquery.Client()

class DataSource:

    def __init__(
            self,
            source: EncodedSource,
            client: bigquery.client
    ):
        self._source = source
        self._client = client
        self._encoded_sources = self._get_dependencies()

        # def build(self):
    #     unmets = self._fetch_ummet_dependencies()
    #     if unmets:

    def encoded_sources(self):
        return self._encoded_sources

    def _get_dependencies(self) -> Dict[str, EncodedSource]:

        return self._source.all_encoded_sources_by_name()

        # all_dependencies = Set()
        # with ThreadPoolExecutor(max_workers=1) as executor:
        #     future = executor.submit(pow, 323, 1235)
        #
        #     print(future.result())
        #
        # for source in par(self._source.hashed_sources()):
        #     if source
        # return client.get_table('rmartin_bq_cache.' + self._source.encoded_sources())



# def get_df(client, project, dataset, table, day):
#     table_standard_sql = f'{project}.{dataset}.{table}'
#     sql = f"""
#         select * from `{table_standard_sql}` where day = "{day}"
#     """
#     query_config = bigquery.QueryJobConfig()
#
#     tic = time.perf_counter()
#     df = client.query(sql, job_config=query_config).to_dataframe()
#     toc = time.perf_counter()
