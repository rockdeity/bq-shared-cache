import click

from bq.data_source import DataSource
from concurrent.futures.thread import ThreadPoolExecutor
import google.api_core
from google.cloud import bigquery
import json
import logging
from resources.test_source_sql import date_dim_query, date_dim_query_sub_cached, offering_query, complex_query, offering_query_cached
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
    #datasource = DataSource(EncodedSource.from_str(offering_query_cached, prefix="cached_"))
    with open("resources/complex.sql", "r") as sql_file:
        datasource = DataSource(EncodedSource.from_str(sql_file.read(), prefix="cached_"))

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
                    logger.info(f"dependencies met for hash:{hashed}")
                except google.api_core.exceptions.NotFound as e:
                    logger.info(f"dependencies NOT met for hash:{hashed}, building...")
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
        query_job = client.query(
            sql,
            job_config=query_config,
        )
        result = query_job.result()
        toc = time.perf_counter()
        logger.info(f"query took:{toc - tic} seconds")
        logger.info(f"total bytes processed:{query_job.total_bytes_processed:,}")
        logger.info(f"result:{result}")
        # logger.info(f"df:{df}")
        return result

    tic = time.perf_counter()
    datasource.apply_dependency_first(apply_func=apply_to_encoded)
    toc = time.perf_counter()
    logger.info(f"completed:{completed}")
    logger.info(f"TOTAL queries took:{toc - tic} seconds")




if __name__ == '__main__':
    main()