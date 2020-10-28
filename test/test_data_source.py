
import json
import sys
import unittest


from resources.test_source_sql import complex_query, basic_str, basic_whitespace_str, cte_1, cte_2, join_clause, \
    date_dim_str

sys.path.append("..")
from src.source import EncodedSource
from src.bq.data_source import DataSource


class Test(unittest.TestCase):

    def test_from_encode(self):
        encoded = EncodedSource.from_str(cte_1)
        self.assertIsNotNone(encoded)

    def test_datasource(self):
        datasource = DataSource(EncodedSource.from_str(cte_1))
        self.assertIsNotNone(datasource)
    #
    def test_basicsource(self):
        datasource = DataSource(EncodedSource.from_str(date_dim_str))
        for hashed, source in datasource.encoded_sources().items():
            print(f"hash:{hashed} source:{source.encoded_sources()}")


if __name__ == '__main__':
    unittest.main()