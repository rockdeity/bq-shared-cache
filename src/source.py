# Module for handling query source code

from __future__ import annotations
import functools
import logging
import sqlparse
from typing import Tuple, Union, Dict, List

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.INFO)
logger = logging.getLogger(__name__)


class Source:

    def __init__(self,
                 source: str):
        self._source = source

    def source(self) -> str:
        return self._source


def serialize_tokens(tokens: sqlparse.tokens):
    import re

    out = ""
    for token in tokens:
        if token.is_group:
            out += serialize_tokens(token.tokens)
        else:
            out += re.sub('\\s +', ' ', token.value)  # trim extra whitespace
    return out

class ParsedSource:

    def __init__(self,
                 source: Source):
        self._source = source
        self._parsed_source = self.__parse()

    def source(self) -> Source:
        return self._source

    def parsed_source(self) -> list[Tuple[sqlparse.sql.Statement]]:
        return self._parsed_source

    def serialize(self) -> str:
        raw_string = ";".join([serialize_tokens(statement.tokens) for statement in
                               [tuple for tuple in self._parsed_source]])
        return sqlparse.format(raw_string, reindent=True, keyword_case='upper')

    def __parse(self) -> list[Tuple[sqlparse.sql.Statement]]:
        split_statements = []
        for split in sqlparse.split(self._source.source()):
            parsed_split = sqlparse.parse(sqlparse.format(split, reindent=True, keyword_case='upper'))
            split_statements.extend(parsed_split)
        return split_statements



class EncodedSource:

    def __init__(self,
                 parsed_source: ParsedSource):
        self._parsed_source = parsed_source
        self._unnamed_statements, self._ctes = self._extract_ctes()
        self._dependency_map = self._map_dependencies()
        self._encoded_source = self.__encode()

    def parsed_source(self) -> ParsedSource:
        return self._parsed_source

    def encoded_source(self) -> str:
        return self._encoded_source

    def ctes(self) -> Dict[str, List[Tuple[sqlparse.sql.Statement]]]:
        return self._ctes

    def unnamed_statements(self):
        return self._unnamed_statements

    # return dictionary of encoded hash and statement, keyed by statement name
    def __encode(self) -> Dict[str, Tuple[str, sqlparse.tokens]]:

        ctes = {}
        encoded_ctes = {}
        encode_order = ctes.keys()
        # make sure we build in dependency order
        def dependency_compare(x: str, y: str, dependencies: Dict[str, str] = self._dependency_map):
            if dependencies.get(y) and x in dependencies[y]:
                return -1
            elif dependencies.get(x) and y in dependencies[x]:
                return 1
            return 0

        def replace_encoded(tokens: sqlparse.tokens):
            for token in tokens:
                encoded_token = encoded_ctes.get(token.value)
                if encoded_token:
                    token.value = f"`{encoded_token}`"
                if token.is_group:
                    replace_encoded(token.tokens)

        # encode expressions
        for encode_key in sorted(encode_order, key=functools.cmp_to_key(dependency_compare)):
            tokens = self._ctes[encode_key]
            replace_encoded(tokens)
            assert(not encoded_ctes.get(encode_key))
            formatted_cte = sqlparse.format(serialize_tokens(tokens))
            import hashlib
            hasher = hashlib.sha1()
            hasher.update(formatted_cte.encode('utf-8'))
            hashed = hasher.hexdigest()
            encoded_ctes[encode_key] = (hashed, formatted_cte)

        return encoded_ctes  # return the final encoded statement

    def _extract_ctes(self):
        dependencies = {}
        unnamed = []
        ctes = {}
        for statement in self._parsed_source.parsed_source():
            for cte_name, tokens in extract_ctes(statement.tokens):  # [x for x in statement.tokens if not x.is_whitespace]):
                assert (ctes.get(cte_name) is None)
                if not cte_name:
                    unnamed.append(tokens)
                else:
                    ctes[cte_name] = tokens
        return unnamed, dependencies

    def _map_dependencies(self):
        dependencies = {}
        for cte_name, tokens in self._ctes.items():
            for token in tokens:
                # TODO: might need recursive flatten here
                for flat_token in token.flatten():
                    dependency = flat_token.value
                    dependency_list = dependencies.get(cte_name, [])
                    # see if we have a query which maps to this name
                    if dependency in ctes.keys():
                        dependency_list.append(dependency)
                    dependencies[cte_name] = dependency_list
                dependencies[cte_name] = dependency_list
        return dependencies


def extract_ctes(tokens: sqlparse.tokens) -> Union[str, sqlparse.tokens]:
    remaining_tokens = []
    for token in tokens:
        # from https://www.programcreek.com/python/?code=dbcli%2Flitecli%2Flitecli-master%2Flitecli%2Fpackages%2Fparseutils.py
        if isinstance(token, sqlparse.sql.IdentifierList):
            item_list = token.get_identifiers()
            for identifier in item_list:
                # Sometimes Keywords (such as FROM ) are classified as
                # identifiers which don't have the get_real_name() method.
                try:
                    schema_name = identifier.get_parent_name()
                    real_name = identifier.get_real_name()
                    alias = identifier.get_alias()
                except AttributeError:
                    continue
                cte_tokens = identifier.tokens # [x for x in identifier.tokens if not x.is_whitespace]
                found_as = False
                for cte_token in cte_tokens:
                    #logger.info(f"cte_token:{cte_token}")
                    if cte_token.value == 'AS':
                        found_as = True
                    elif found_as and type(cte_token) == sqlparse.sql.Parenthesis:
                        between_parens = cte_token.tokens[1:-1] # [x for x in cte_token.tokens if not x.is_whitespace][1:-1]
                        yield real_name,  between_parens

        else:
            if token.value == 'WITH':
                remaining_tokens = []
            else:
                remaining_tokens.append(token)

    yield None, remaining_tokens # remaining_tokens[x for x in remaining_tokens if not x.is_whitespace]


