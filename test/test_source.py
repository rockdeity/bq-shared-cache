from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, TimeoutError
import logging
from multiprocessing import Queue, Manager
import sys
import time
import timeout_decorator
import unittest

sys.path.append("..")
from src.source import Source, EncodedSource, ParsedSource


class Test(unittest.TestCase):

    def test_source_base(self):
        self.assertFalse(False)

    def test_source(self):
        source_str = "SELECT * FROM `universe.galaxy.system`"
        source = Source(source_str)
        self.assertIsNotNone(source)
        self.assertEqual(source_str, source.source())

    def test_parse(self):
        source_str = "SELECT * FROM `universe.galaxy.system`"
        source = Source(source_str)
        parsed = ParsedSource(source)
        self.assertIsNotNone(parsed)
        self.assertEqual(source_str, parsed.source().source())
        self.assertNotEqual(source_str, parsed.parsed_source())
        #new_source = source.encode()

    def test_encode(self):
        source_str = "SELECT * FROM `universe.galaxy.system`"
        source = Source(source_str)
        parsed_source = ParsedSource(source)
        encoded = EncodedSource(parsed_source)
        self.assertIsNotNone(encoded)
        self.assertEqual(parsed_source, encoded.parsed_source())
        self.assertNotEqual(source_str, encoded.encoded_source())
        #new_source = source.encode()

    def test_match_whitespace_diff(self):
        source_str = "SELECT * FROM `universe.galaxy.system`"
        target_str = "SELECT *    FROM    `universe.galaxy.system`   "
        source = Source(source_str)
        target = Source(target_str)
        encoded_source = EncodedSource(ParsedSource(source))
        encoded_target = EncodedSource(ParsedSource(target))
        self.assertIsNotNone(encoded_source)
        self.assertIsNotNone(encoded_target)
        self.assertNotEqual(source_str, target_str)
        self.assertNotEqual(source.source(), target.source())
        self.assertEqual(encoded_source.encoded_source(), encoded_target.encoded_source())

    def test_cte_basic_parse(self):
        source_str = """
            with cte as (
                SELECT * FROM `universe.galaxy.system`
            ),
            cte2 as (
                SELECT * FROM `country.state.city`
            )
            SELECT * FROM cte JOIN cte2 USING (planet)
            """
        source = Source(source_str)
        encoded = EncodedSource(ParsedSource(source))
        self.assertIsNotNone(encoded)
        self.assertEqual(source_str, encoded.parsed_source().source().source())
        self.assertNotEqual(source_str, encoded.encoded_source())




if __name__ == '__main__':
    unittest.main()