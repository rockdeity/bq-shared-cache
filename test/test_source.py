import sys
import unittest

import sqlparse

from resources.test_source_sql import complex_query, basic_str, basic_whitespace_str, cte_1, cte_2, join_clause, \
    date_dim_query, settings, planning_date_dim_table, planning_week_dim_table, weeks, date_dim_select

sys.path.append("..")
from src.source import Source, EncodedSource, ParsedSource, DecomposedSource


class Test(unittest.TestCase):

    def test_source_base(self):
        self.assertFalse(False)

    def test_source(self):
        source_str = cte_1
        source = Source(source_str)
        self.assertIsNotNone(source)
        self.assertEqual(source_str, source.source())

    def test_parse(self):
        source_str = cte_1
        source = Source(source_str)
        parsed = ParsedSource(source)
        self.assertIsNotNone(parsed)
        self.assertEqual(source_str, parsed.source().source())
        self.assertNotEqual(source_str, parsed.parsed_statements())

    def test_decompose(self):
        source_str = cte_1
        parsed_source = ParsedSource(Source(source_str))
        decomposed = DecomposedSource(parsed_source)
        self.assertIsNotNone(decomposed)
        #self.assertNotEqual(source_str, decomposed.parsed_sources()[0].source().source())
        self.assertEqual(parsed_source.serialize(), decomposed.parsed_sources()[0].serialize())

    def test_encode(self):
        source_str = cte_1
        decomposed_source = DecomposedSource(ParsedSource(Source(source_str)))
        encoded = EncodedSource(decomposed_source)
        self.assertIsNotNone(encoded)
        self.assertEqual(decomposed_source.serialize(), encoded.decomposed_source().serialize())

    def test_match_whitespace_diff(self):
        source_str = "SELECT * FROM `universe.galaxy.system`"
        target_str = "SELECT *    FROM    `universe.galaxy.system`   "
        source = Source(source_str)
        target = Source(target_str)
        encoded_source = EncodedSource(DecomposedSource(ParsedSource(source)))
        encoded_target = EncodedSource(DecomposedSource(ParsedSource(target)))
        self.assertIsNotNone(encoded_source)
        self.assertIsNotNone(encoded_target)
        self.assertNotEqual(source_str, target_str)
        self.assertNotEqual(source.source(), target.source())
        self.assertEqual(encoded_source.encoded_sources(), encoded_target.encoded_sources())

    def test_cte_basic_parse(self):

        source = Source(basic_str)
        self.assertIsNotNone(source)
        parsed_source = ParsedSource(source)
        self.assertIsNotNone(parsed_source)
        self.assertEqual(basic_str, source.source())
        self.assertNotEqual(basic_str, parsed_source.parsed_statements())

    def test_cte_parse_matches(self):
        source_str = basic_str
        source = Source(source_str)
        parsed_source = ParsedSource(source)

        source_2 = Source(basic_whitespace_str)
        self.assertIsNotNone(source_2)
        parsed_source_2 = ParsedSource(source_2)
        self.assertIsNotNone(parsed_source_2)
        self.assertEqual(basic_whitespace_str, source_2.source())
        self.assertNotEqual(basic_whitespace_str, parsed_source_2.parsed_statements())
        self.assertEqual(parsed_source.serialize(), parsed_source_2.serialize())

    def test_cte_complex_parse(self):
        source_str = complex_query
        source = Source(source_str)
        self.assertIsNotNone(source)
        parsed_source = ParsedSource(source)
        self.assertIsNotNone(parsed_source)
        self.assertEqual(source_str, source.source())
        self.assertNotEqual(source_str, parsed_source.parsed_statements())

    def test_basic_decompose(self):

        decomposed_source = DecomposedSource(ParsedSource(Source(basic_str)))
        self.assertIsNotNone(decomposed_source)
        parsed_sources = decomposed_source.parsed_sources()
        self.assertEqual(ParsedSource(Source(cte_1)).serialize(), parsed_sources[0].serialize())
        self.assertEqual(ParsedSource(Source(cte_2)).serialize(), parsed_sources[1].serialize())
        self.assertEqual(ParsedSource(Source(join_clause)).serialize(), parsed_sources[2].serialize())

    def test_date_decompose(self):

        self.maxDiff = None
        decomposed_source = DecomposedSource(ParsedSource(Source(date_dim_query)))
        self.assertIsNotNone(decomposed_source)
        parsed_sources = decomposed_source.parsed_sources()
        self.assertEqual(ParsedSource(Source(settings)).serialize(reindent=True), parsed_sources[0].serialize(reindent=True))
        self.assertEqual(ParsedSource(Source(planning_date_dim_table)).serialize(reindent=True), parsed_sources[1].serialize(reindent=True))
        self.assertEqual(ParsedSource(Source(planning_week_dim_table)).serialize(reindent=True), parsed_sources[2].serialize(reindent=True))
        self.assertEqual(ParsedSource(Source(weeks)).serialize(reindent=True), parsed_sources[3].serialize(reindent=True))
        self.assertEqual(ParsedSource(Source(date_dim_select)).serialize(reindent=True), parsed_sources[4].serialize(reindent=True))


    def test_cte_basic_encode(self):

        source_str = basic_str
        source = Source(source_str)
        encoded = EncodedSource(DecomposedSource(ParsedSource(source)))
        self.assertIsNotNone(encoded)

    def test_cte_date_dim_encode(self):
        source_str = date_dim_query
        encoded_source_root = EncodedSource(DecomposedSource(ParsedSource(Source(source_str))))
        self.assertIsNotNone(encoded_source_root)
        #self.assertEqual(complex_query, all_encoded_sources.parsed_sources().source())
        for hash, encoded_source in zip(encoded_source_root.hashed_sources(), encoded_source_root.encoded_sources()):
            print(
                f"-----------------------------------\n"
                f"{hash}"
                f"\n-----------------------------------\n"
                f"{encoded_source}")
        main_statement_encoded = EncodedSource.from_str(date_dim_select)
        self.assertEqual(len(main_statement_encoded.all_encoded_sources()), 1)
        # print(f"main_statement_encoded.all_encoded_sources(){main_statement_encoded.all_encoded_sources()}")
        # print(f"encoded_source_root.all_encoded_sources(){encoded_source_root.all_encoded_sources()}")

    def test_cte_complex_encode(self):
        encoded_source = EncodedSource(DecomposedSource(ParsedSource(Source(complex_query))))
        self.assertIsNotNone(encoded_source)
        #self.assertEqual(complex_query, all_encoded_sources.parsed_sources().source())
        for hash, encoded_source in zip(encoded_source.hashed_sources(), encoded_source.encoded_sources()):
            print(
                f"-----------------------------------\n"
                f"{hash}"
                f"\n-----------------------------------\n"
                f"{encoded_source}")

    def test_cte_encode_matches(self):
        source_str = basic_str
        encoded_source = EncodedSource(DecomposedSource(ParsedSource(Source(source_str))))
        source_str_2 = basic_whitespace_str
        encoded_source_2 = EncodedSource(DecomposedSource(ParsedSource(Source(source_str_2))))
        self.assertIsNotNone(encoded_source_2)
        self.assertEqual(encoded_source.encoded_sources(), encoded_source_2.encoded_sources())


if __name__ == '__main__':
    unittest.main()