# Module for handling query source code

from __future__ import annotations
import sqlparse


class Source:

    def __init__(self,
                 source: str):

        self._source = source

    def source(self) -> str:
        return self._source


class ParsedSource:

    def __init__(self,
                 source: Source):
        self._source = source
        self._parsed_source = self.__parse()

    def source(self) -> Source:
        return self._source

    def parsed_source(self) -> str:
        return self._parsed_source

    def __parse(self) -> str:
        return sqlparse.format(self._source.source(), reindent=True, keyword_case='upper')

    def split(self) -> list[tuple[str, ParsedSource]]:
        return sqlparse.parse(self._parsed_source)


class EncodedSource:

    def __init__(self,
                 parsed_source: ParsedSource):
        self._parsed_source = parsed_source
        self._encoded_source = self.__encode()

    def parsed_source(self) -> ParsedSource:
        return self._parsed_source

    def encoded_source(self) -> str:
        return self._encoded_source

    def __encode(self) -> str:
        #TODO base64
        return self._parsed_source.parsed_source()


