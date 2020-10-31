
import json
import sys
from typing import Union, Dict, List
import unittest

from resources.test_source_sql import complex_query, basic_str, basic_whitespace_str, cte_1, cte_2, join_clause, \
    date_dim_query, settings, planning_date_dim_table, planning_week_dim_table, weeks, date_dim_query_sub_cached

sys.path.append("..")
from src.source import EncodedSource, ParsedSource, Source, hash_reference
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
        datasource = DataSource(EncodedSource.from_str(date_dim_query))
        for hashed, source in datasource.all_encoded_sources().items():
            print(f"hash:{hashed} source:{source.encoded_sources()}")

    def test_cte_date_dim_encode(self):
        source_str = date_dim_query
        encoded_source_root = EncodedSource.from_str(source_str)
        datasource = DataSource(encoded_source_root)
        self.assertIsNotNone(datasource)

    def test_simplified_complex_encode(self):
        with open("../resources/complex_simplified.sql", "r") as sql_file:
            datasource = DataSource(EncodedSource.from_str(sql_file.read(), prefix="cached_"))
            self.assertIsNotNone(datasource)

    def test_complex_encode(self):
        source_str = complex_query
        encoded_source_root = EncodedSource.from_str(source_str)
        datasource = DataSource(encoded_source_root)
        self.assertIsNotNone(datasource)

    def test_apply_dependency_first(self):
        source_str = cte_1
        encoded_source_root = EncodedSource.from_str(source_str)
        datasource = DataSource(encoded_source_root)
        run_order = []
        expected_run_order = [cte_1]
        def apply_to_encoded(hash: str, source: str, run_order: List[str] = run_order):
            #print(f"hash:{hash}, source:{source}")
            run_order.append(source)
        datasource.apply_dependency_first(apply_to_encoded)
        self.assertEqual(run_order, expected_run_order)

    def test_apply_dependency_first_basic(self):
        source_str = basic_str
        encoded_source_root = EncodedSource.from_str(source_str)
        datasource = DataSource(encoded_source_root)
        run_order = []
        hashed_cte_1 = EncodedSource.from_str(cte_1).hashed_sources()[-1]
        hashed_cte_2 = EncodedSource.from_str(cte_2).hashed_sources()[-1]
        encoded_join = f"WITH  cte AS (SELECT * FROM `{hashed_cte_1}`),\n cte2 AS (SELECT * FROM `{hashed_cte_2}`)\n{join_clause}"
        expected_run_order = [cte_1, cte_2, encoded_join]
        def apply_to_encoded(hash: str, source: str, run_order: List[str] = run_order):
            #print(f"hash:{hash}, source:{source}")
            run_order.append(source)
        datasource.apply_dependency_first(apply_to_encoded)
        self.assertEqual(expected_run_order, run_order)

    def test_apply_dependency_first_date(self):
        self.maxDiff = None
        source_str = date_dim_query
        encoded_source_root = EncodedSource.from_str(source_str)
        datasource = DataSource(encoded_source_root)
        run_order = []
        encoded_settings = EncodedSource.from_str(settings)
        encoded_date_dim = EncodedSource.from_str(planning_date_dim_table)
        parsed_week_dim = ParsedSource(Source(planning_week_dim_table)).serialize() #.replace("planning_date_dim_table", f"`{encoded_date_dim.hashed_sources()[-1]}`")
        encoded_replaced_date_dim = f"WITH  planning_date_dim_table AS (SELECT * FROM `{encoded_date_dim.hashed_sources()[-1]}`)\n"
        encoded_week_dim = EncodedSource.from_str(f"WITH planning_date_dim_table AS ({planning_date_dim_table}) {planning_week_dim_table}")
        encoded_replaced_week_dim = f"WITH  planning_week_dim_table AS (SELECT * FROM `{encoded_week_dim.hashed_sources()[-1]}`)\n"
        replaced_weeks = ParsedSource(Source(weeks)).serialize() #.replace("planning_week_dim_table", f"`{encoded_replaced_week_dim.hashed_sources()[-1]}`")
        expected_run_order = [encoded_date_dim.encoded_sources()[-1], encoded_replaced_date_dim + parsed_week_dim,
                              encoded_replaced_week_dim + replaced_weeks, encoded_source_root.encoded_sources()[-1]]
        already_run = {}
        def apply_to_encoded(hash: str, source: str, run_order: List[str] = run_order, already_run: Dict[str, str] = already_run):
            #print(f"hash:{hash}, source:{source}")
            run = already_run.get(hash)
            if not run:
                already_run[hash] = source
                run_order.append(source)
        datasource.apply_dependency_first(apply_to_encoded)
        self.assertEqual(expected_run_order, run_order)

    def test_apply_dependency_first_date_cached(self):
        self.maxDiff = None
        source_str = date_dim_query_sub_cached
        #encoded_source_root = EncodedSource.from_str(source_str, prefix="cached_")
        encoded_source_root = EncodedSource.from_str(source_str, prefix="cached_")
        datasource = DataSource(encoded_source_root)
        run_order = []
        encoded_settings = EncodedSource.from_str(settings)
        encoded_date_dim = EncodedSource.from_str(planning_date_dim_table)
        parsed_week_dim = ParsedSource(Source(planning_week_dim_table)).serialize() #.replace("planning_date_dim_table", f"`{encoded_date_dim.hashed_sources()[-1]}`")
        encoded_replaced_date_dim = f"WITH planning_date_dim_table AS (SELECT * FROM `{encoded_date_dim.hashed_sources()[-1]}`)\n"
        encoded_week_dim = EncodedSource.from_str(f"WITH planning_date_dim_table AS ({planning_date_dim_table}) {planning_week_dim_table}")
        encoded_replaced_week_dim = f"WITH planning_week_dim_table AS (SELECT * FROM `{encoded_week_dim.hashed_sources()[-1]}`)\n"
        replaced_weeks = ParsedSource(Source(weeks)).serialize() #.replace("planning_week_dim_table", f"`{encoded_replaced_week_dim.hashed_sources()[-1]}`")
        expected_run_order = [encoded_date_dim.encoded_sources()[-1], encoded_replaced_date_dim + parsed_week_dim,
                              encoded_replaced_week_dim + replaced_weeks, encoded_source_root.encoded_sources()[-1]]
        already_run = {}
        def apply_to_encoded(hash: str, source: str, run_order: List[str] = run_order, already_run: Dict[str, str] = already_run):
            print(f"hash:{hash}, source:{source}")
            run = already_run.get(hash)
            if not run:
                already_run[hash] = source
                run_order.append(source)
        datasource.apply_dependency_first(apply_to_encoded)
        self.assertEqual(expected_run_order, run_order)



if __name__ == '__main__':
    unittest.main()