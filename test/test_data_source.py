import sys
import unittest

import sqlparse

from test.test_source_sql import complex_query, basic_str, basic_whitespace_str, cte_1, cte_2, join_clause

sys.path.append("..")
from src.source import EncodedSource
from src.bq.data_source import DataSource


class Test(unittest.TestCase):

    def test_from_encode(self):
        encoded = EncodedSource.from_str(cte_1)
        self.assertIsNotNone(encoded)
        print(encoded.all_encoded_sources_by_name())

    def test_datasource(self):
        datasource = DataSource(EncodedSource.from_str(cte_1))
        print(datasource.encoded_sources())


if __name__ == '__main__':
    unittest.main()