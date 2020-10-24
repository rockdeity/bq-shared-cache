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


class ParsedSource:

    def __init__(self,
                 source: Source):
        self._source = source
        self._parsed_source = self.__parse()

    def source(self) -> Source:
        return self._source

    def parsed_source(self) -> list[Tuple[sqlparse.sql.Statement]]:
        return self._parsed_source

    def __parse(self) -> list[Tuple[sqlparse.sql.Statement]]:
        split_statements = []
        formatted_source = sqlparse.format(self._source.source(), reindent=True, keyword_case='upper')
        for split in sqlparse.split(formatted_source):
            parsed_split = sqlparse.parse(split)
            split_statements.append(parsed_split)
        return parsed_split

    # def split(self) -> list[tuple[str, ParsedSource]]:
    #     return self._parsed_source.split


class EncodedSource:

    def __init__(self,
                 parsed_source: ParsedSource):
        self._parsed_source = parsed_source
        self._encoded_source = self.__encode()

    def parsed_source(self) -> ParsedSource:
        return self._parsed_source

    def encoded_source(self) -> str:
        return self._encoded_source

    def ctes(self) -> Dict[str, List[Tuple[sqlparse.sql.Statement]]]:
        return self._ctes

    def _extract_ctes(self, tokens: sqlparse.tokens) -> Union[str, sqlparse.tokens]:
        cte_dict = {}
        ret_tokens = tokens
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
                    #logger.info(f"identifier real_name:{real_name} alias:{alias}")
                    #with_ctes[real_name] = tokens
                    cte_tokens = [x for x in identifier.tokens if not x.is_whitespace]
                    found_as = False
                    for cte_token in cte_tokens:
                        #logger.info(f"cte_token:{cte_token}")
                        if cte_token.value == 'AS':
                            found_as = True
                        elif found_as and type(cte_token) == sqlparse.sql.Parenthesis:
                            between_parens = [x for x in cte_token.tokens if not x.is_whitespace][1:-1]
                            yield real_name,  between_parens

            else:
                if token.value == 'WITH':
                    remaining_tokens = []
                else:
                    remaining_tokens.append(token)

        yield None, [x for x in remaining_tokens if not x.is_whitespace]

    def __encode(self) -> str:
        # TODO base64
        encoded = ""
        ctes = {}
        for statement in self._parsed_source.parsed_source():
            logger.info(f"statement:{statement}")
            remaining_tokens = [x for x in statement.tokens if not x.is_whitespace]
            queries = dict()
            for cte_name, tokens in self._extract_ctes(remaining_tokens):
                logger.info(f"cte_name:{cte_name} tokens:{tokens}")
                assert(queries.get(cte_name) is None)
                ctes[cte_name] = tokens
            dependencies = {}
            for cte_name, tokens in ctes.items():
                for token in tokens:
                    #if isinstance(token, sqlparse.sql.Identifier):
                    for flat_token in token.flatten():
                        dependency = flat_token.value
                        dependency_list = dependencies.get(cte_name, [])
                        # see if we have a query which maps to this name
                        if dependency in ctes.keys():
                            dependency_list.append(dependency)
                        dependencies[cte_name] = dependency_list
                    dependencies[cte_name] = dependency_list
        self._ctes = ctes

        encoded_ctes = {}
        encode_order = ctes.keys()
        # make sure we build in dependency order
        def dependency_compare(x: str, y: str):
            if dependencies.get(y) and x in dependencies[y]:
                return -1
            elif dependencies.get(x) and y in dependencies[x]:
                return 1
            return 0
        encode_order = sorted(encode_order, key=functools.cmp_to_key(dependency_compare))

        def replace_encoded(tokens: sqlparse.tokens):
            for token in tokens:
                encoded_token = encoded_ctes.get(token.value)
                if encoded_token:
                    token.value = f"`{encoded_token}`"
                if token.is_group:
                    replace_encoded(token.tokens)

        def serialize_parsed(tokens: sqlparse.tokens):
            out = ""
            for token in tokens:
                if token.is_group:
                    out += serialize_parsed(token.tokens)
                else:
                    out += token.value
            return out

        for encode_key in encode_order:
            tokens = ctes[encode_key]
            replace_encoded(tokens)
            assert(not encoded_ctes.get(encode_key))
            formatted_cte = sqlparse.format(serialize_parsed(tokens))
            import hashlib
            hasher = hashlib.sha1()
            hasher.update(formatted_cte.encode('utf-8'))
            hashed = hasher.hexdigest()
            encoded_ctes[encode_key] = hashed

        logger.info(f"encoded:{encoded}")
        return encoded
