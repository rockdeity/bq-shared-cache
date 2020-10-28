# abstraction data interface for BQ queries
from datetime import datetime, timedelta
import logging
import time
import sys
from typing import Tuple, Union, Dict, List, Set, Callable, Any
sys.path.append(".")
from src.source import EncodedSource

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.INFO)
logger = logging.getLogger(__name__)

# yesterday = datetime.now() - timedelta(days=1, hours=7)
#
# yesterdays_date = yesterday.strftime("%Y-%m-%d")
# #print(yesterdays_date)

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

    def encoded_source(self):
        return self._source

    def all_encoded_sources(self) -> Dict[str, EncodedSource]:
        return self._encoded_sources

    def apply_dependency_first(self, apply_func: Callable[[str, str], int]):
        _apply_dependency_first(self._source, apply_func)

    def _get_dependencies(self) -> Dict[str, EncodedSource]:

        return self._source.all_encoded_sources_by_name()

def _apply_dependency_first(encoded_source: EncodedSource, apply_func: Callable[[str, str], int]):

    dependencies = encoded_source.direct_encoded_dependencies()
    for dependency in dependencies:
        _apply_dependency_first(dependency, apply_func)
    last_statement = encoded_source.encoded_sources()[-1]
    last_hash = encoded_source.hashed_sources()[-1]
    apply_func(last_hash, last_statement)








