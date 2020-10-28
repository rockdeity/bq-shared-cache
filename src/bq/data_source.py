# abstraction data interface for BQ queries
from datetime import datetime, timedelta
import logging
import time
import sys
from typing import Tuple, Union, Dict, List, Set
sys.path.append(".")
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
            #client: bigquery.client
    ):
        self._source = source
        #self._client = client
        self._encoded_sources = self._get_dependencies()

        # def build(self):
    #     unmets = self._fetch_ummet_dependencies()
    #     if unmets:

    def encoded_sources(self) -> Dict[str, EncodedSource]:
        return self._encoded_sources

    def _get_dependencies(self) -> Dict[str, EncodedSource]:

        return self._source.all_encoded_sources_by_name()






