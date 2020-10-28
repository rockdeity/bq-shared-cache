import click

from bq.data_source import DataSource
from concurrent.futures.thread import ThreadPoolExecutor
import google.api_core
from google.cloud import bigquery
import json
import logging
from resources.test_source_sql import date_dim_query
from source import EncodedSource
import time
from typing import Union, Dict, List

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.INFO)
logger = logging.getLogger(__name__)

@click.command()
@click.option("--timeout", help="Seconds to wait for the bigquery job to complete", type=float,  default=1800)
@click.option("--project", help="gcp project to use", default="massive-clone-705")
@click.option("--dataset",  help="gcp project to use", default="rmartin_bq_cache")
def main(timeout, project, dataset):
    client = bigquery.Client(project=project)
    dataset_ref = client.dataset(dataset)
    datasource = DataSource(EncodedSource.from_str(date_dim_query))

    completed = {}
    running = {}

    def apply_to_encoded(hashed: str, source: str, running: List[str] = running, completed: Dict[str, str] = completed):
        this_completed = completed.get(hashed)
        if not this_completed:
            this_running = running.get(hashed)
            if not this_running:
                try:
                    table_ref = client.get_table(f"{dataset}.{hashed}")
                    completed[hashed] = table_ref
                except google.api_core.exceptions.NotFound as e:
                    logger.error(f"err:{e}")
                    running[hashed] = True
                    completed[hashed] = do_query(hashed, source)

    def do_query(hash, sql):
        logger.info(f"sql:{sql}")
        tic = time.perf_counter()
        query_config = google.cloud.bigquery.job.QueryJobConfig(
            destination=f"{project}.{dataset}.{hash}",
            default_dataset=dataset_ref,
            priority=bigquery.QueryPriority.INTERACTIVE
        )
        df = client.query(
            sql,
            job_config=query_config,
        ) #.to_dataframe()
        toc = time.perf_counter()
        logger.info(f"took:{toc - tic} seconds")
        # logger.info(f"df:{df}")
        return df

    datasource.apply_dependency_first(apply_func=apply_to_encoded)
    logger.info(f"completed:{completed}")



    # all_dependencies = Set()
    # with ThreadPoolExecutor(max_workers=1) as executor:
    #     future = executor.submit(pow, 323, 1235)
    #     future_to_url = {executor.submit(load_url, url, 60): hashed, source for hashed, source in unbuilt.items()}
    #     for future in concurrent.futures.as_completed(future_to_url):
    #         url = future_to_url[future]
    #         try:
    #             data = future.result()
    #         except Exception as exc:
    #             print('%r generated an exception: %s' % (url, exc))
    #         else:
    #             print('%r page is %d bytes' % (url, len(data)))
    #     #CREATE TABLE `massive-clone-705`.`triple_z_demand_prediction_prod`.`predictions_v7_product` (
    #     print(future.result())

    #
    # for hash, source in unbuilt.items():
    #     sql = source.encoded_sources()[0]
    #     logger.info(f"sql:{sql}")
    #     tic = time.perf_counter()
    #     query_config = google.cloud.bigquery.job.QueryJobConfig(
    #         destination=f"{project}.{dataset}.{hash}",
    #         default_dataset=dataset_ref,
    #         priority=bigquery.QueryPriority.INTERACTIVE
    #     )
    #     df = client.query(
    #         sql,
    #         job_config=query_config,
    #     ).to_dataframe()
    #     toc = time.perf_counter()
    #     logger.info(f"took:{toc - tic} seconds")
    #     logger.info(f"df:{df}")
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



if __name__ == '__main__':
    main()