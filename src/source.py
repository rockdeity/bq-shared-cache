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

def clean(tokens: sqlparse.tokens, recurse=False) -> sqlparse.tokens:
    ret_tokens = []
    # trim extra whitespace
    prev_token = None
    for token in tokens:
        if token.is_group and recurse:
            ret_tokens.extend(clean(token.tokens, recurse=recurse))
        elif ((not token.is_whitespace or not prev_token or not prev_token.is_whitespace)
                # trim comments
                and not token.match(sqlparse.sql.Comment, None)
                and not token.match(sqlparse.tokens.Comment.Single, None)
                and not token.match(sqlparse.tokens.Comment.Multiline, None)):  #isinstance(token, sqlparse.sql.Comment)):
            ret_tokens.append(token)
        prev_token = token
    return ret_tokens

def serialize_tokens(tokens: sqlparse.tokens):

    out = ""
    cleaned = clean(tokens)
    for token in cleaned:
        if token.is_group:
            out += serialize_tokens(token.tokens)
        else:
            out += token.value
    return out

class ParsedSource:

    def __init__(self,
                 source: Source):
        self._source = source
        self._parsed_statements = self.__parse()

    def source(self) -> Source:
        return self._source

    def parsed_statements(self) -> List[sqlparse.sql.Statement]:
        return self._parsed_statements

    def serialize(self) -> str:
        raw_string = ";".join([serialize_tokens(statement.tokens) for statement in
                               [tuple for tuple in self._parsed_statements]])
        return sqlparse.format(raw_string, reindent=True, keyword_case='upper')

    def __parse(self) -> List[sqlparse.sql.Statement]:
        split_statements = []
        for split in sqlparse.split(self._source.source()):
            parsed_split = sqlparse.parse(split)  # sqlparse.format(split, reindent=True, keyword_case='upper'))
            for statement in parsed_split:
                serialized = serialize_tokens(statement.tokens)
                stripped_tokens = sqlparse.parse(serialized)
                split_statements.extend(stripped_tokens)
        return split_statements

    def extract_statements(self) -> List[Dict[str, sqlparse.tokens]]:
        statements = []
        for statement in self.parsed_statements():
            ctes = {}
            for cte_name, tokens in extract_statements(statement.tokens):
                assert (ctes.get(cte_name) is None)
                ctes[cte_name] = tokens
            statements.append(ctes)
        return statements


class DecomposedSource:

    def __init__(self,
                 parsed_source: ParsedSource,
                 top_level_statements: Dict[str, DecomposedSource] = None,
                 extract_statements = True,
                 alias: str = None):
        self._alias = alias
        self._dependencies = []
        self._parsed_sources = []
        if extract_statements:
            for statements in parsed_source.extract_statements():
                for name, tokens in statements.items():
                    sub_source = ParsedSource(Source(serialize_tokens(tokens)))
                    decomposed_dependencies = self._decompose_dependencies(
                        sub_source,
                        top_level_statements=top_level_statements or statements)
                    self._dependencies.extend(decomposed_dependencies)
                    self._parsed_sources.append(sub_source)
        else:
            for statement in parsed_source.parsed_statements():
                sub_source = ParsedSource(Source(serialize_tokens(statement.tokens)))
                decomposed_dependencies = self._decompose_dependencies(
                    sub_source,
                    top_level_statements=top_level_statements)
                self._dependencies.extend(decomposed_dependencies)
                self._parsed_sources.append(sub_source)


    def parsed_sources(self) -> List[ParsedSource]:
        return self._parsed_sources

    def statements(self) -> List[sqlparse.tokens]:
        return self._parsed_sources.statements()

    def dependencies(self) -> List[Dict[str, DecomposedSource]]:
        return self._dependencies

    def alias(self) -> str:
        return self._alias

    def serialize(self) -> str:
        raw_string = ";".join(parsed_source.serialize() for parsed_source in self.parsed_sources())
        return sqlparse.format(raw_string, reindent=True, keyword_case='upper')

    def _decompose_dependencies(self,
                                parsed_source: ParsedSource,
                                top_level_statements: Dict[str, DecomposedSource]) -> List[Dict[str, DecomposedSource]]:

        all_statement_dependencies = []
        # recursively decompose statements by dependency
        for statement in parsed_source.parsed_statements():
            statement_dependencies = {}
            for dependency in map_dependencies(statement, known_aliases=list(top_level_statements.keys())):
                tokens = top_level_statements.get(dependency)
                sub_source = ParsedSource(Source(serialize_tokens(tokens)))
                statement_dependencies[dependency] = DecomposedSource(
                    sub_source,
                    top_level_statements=top_level_statements,
                    alias=dependency,
                    extract_statements=False)
            all_statement_dependencies.append(statement_dependencies)
        return all_statement_dependencies


class EncodedSource:

    def __init__(self, decomposed_source: DecomposedSource):
        assert(isinstance(decomposed_source, DecomposedSource))
        self._alias = decomposed_source.alias()
        self._decomposed_source = decomposed_source
        self._aliased_source = []
        self._hashed_sources = []
        self._encoded_sources = []
        self._encoded_dependencies = []
        for parsed_source, dependencies in zip(decomposed_source.parsed_sources(), decomposed_source.dependencies()):
            # recursively encode dependencies first
            for alias, dependency in dependencies.items():
                encoded_dependency = EncodedSource(dependency)
                self._encoded_dependencies.append(encoded_dependency)
            serialized = parsed_source.serialize()
            for encoded_dependency in self.encoded_dependencies():
                alias = encoded_dependency.decomposed_source().alias()
                encoded_dependency_sources = encoded_dependency.hashed_sources()
                assert(len(encoded_dependency_sources) == 1)
                #if len(encoded_dependency_sources) > 0:
                serialized = serialized.replace(alias, f"`{encoded_dependency_sources[0]}`")  # only one hashed value per dependency
            self._encoded_sources.append(serialized)
            import hashlib
            hasher = hashlib.sha1()
            hasher.update(serialized.encode('utf-8'))
            hashed = hasher.hexdigest()
            self._hashed_sources.append(hashed)

    def decomposed_source(self) -> DecomposedSource:
        return self._decomposed_source

    # still retains source unencoded, but with encoded dependency references
    def encoded_sources(self) -> List[str]:
        return self._encoded_sources

    # hashes all sources, included encoded dependency references
    def hashed_sources(self) -> List[str]:
        return self._hashed_sources

    def encoded_dependencies(self) -> List[EncodedSource]:
        return self._encoded_dependencies

    # return dictionary of encoded hash and statement, keyed by statement name
    # def __encode(self) -> str:
    #
    #     serialized = self.decomposed_source().parsed_sources().serialize()
    #     for encoded_dependency in self.encoded_dependencies():
    #         alias = encoded_dependency.decomposed_source().alias()
    #         encoded_dependency_sources = encoded_dependency.encoded_sources()
    #         assert(len(encoded_dependency_sources) == 1)
    #         serialized.replace(alias, encoded_dependency_sources[0])
    #     return serialized
        # make sure we build in dependency order
        # def dependency_compare(x: DecomposedSource, y: DecomposedSource):
        #     if y.dependencies().get(x.alias()):
        #         return -1
        #     elif x.dependencies().get(y.alias()):
        #         return 1
        #     return 0

        # def replace_encoded(tokens: sqlparse.tokens):
        #     for token in tokens:
        #         encoded_token = encoded_ctes.get(token.value)
        #         if encoded_token:
        #             token.value = f"`{encoded_token}`"
        #         if token.is_group:
        #             replace_encoded(token.tokens)
        #
        # # encode expressions
        # for encode_key in sorted(encode_order, key=functools.cmp_to_key(dependency_compare)):
        #     tokens = self._ctes[encode_key]
        #     replace_encoded(tokens)
        #     assert(not encoded_ctes.get(encode_key))
        #     formatted_cte = sqlparse.format(serialize_tokens(tokens))
        #     import hashlib
        #     hasher = hashlib.sha1()
        #     hasher.update(formatted_cte.encode('utf-8'))
        #     hashed = hasher.hexdigest()
        #     encoded_ctes[encode_key] = (hashed, formatted_cte)
        #
        # return encoded_ctes  # return the final encoded statement


def map_dependencies(statement: sqlparse.sql.Statement, known_aliases: List[str]) -> List[str]:
    statement_dependencies = []
    # for statement in statements:
    single_dependencies = map_dependencies_single(known_aliases=known_aliases, tokens=statement.tokens)
    #statement_dependencies.append(single_dependencies)
    # if len(ctes) > 0:
    #     dependencies.append(ctes)
    return single_dependencies


def map_dependencies_single(known_aliases: List[str], tokens: sqlparse.tokens) -> List[str]:
    dependency_list = []
    for token in tokens:
        # TODO: might need recursive flatten here
        for flat_token in token.flatten():
            dependency = flat_token.value
            # see if we have a query which maps to this name
            if dependency in known_aliases:
                dependency_list.append(dependency)
        # dependencies[cte_name] = dependency_list
    return dependency_list


def extract_statements(tokens: sqlparse.tokens) -> Union[str, sqlparse.tokens]:
    remaining_tokens = []
    found_with = False
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
                    if cte_token.value.upper() == 'AS':
                        found_as = True
                    # are we defining a CTE identifier?
                    elif found_with:
                        if found_as:
                            if type(cte_token) == sqlparse.sql.Parenthesis:
                                between_parens = list(cte_token.tokens)[1:-1]  # [x for x in cte_token.tokens if not x.is_whitespace][1:-1]
                                yield real_name,  between_parens
                    # non-cte identifier
                    # else:
                    #     yield real_name, cte_token

        else:
            if token.value.upper() == 'WITH':
                remaining_tokens = []
                found_with = True
            else:
                remaining_tokens.append(token)

    yield None, remaining_tokens # remaining_tokens[x for x in remaining_tokens if not x.is_whitespace]


