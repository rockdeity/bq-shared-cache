# Module for handling query source code

from __future__ import annotations

import functools
import logging
from typing import Union, Dict, List, Tuple

import sqlparse

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.INFO)
logger = logging.getLogger(__name__)


def hash_reference(cte_reference: str) -> str:
    return f"`{cte_reference}`"  #  f"( SELECT * FROM `{cte_reference}`)"


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
        if not isinstance(token, sqlparse.sql.Token):
            ret_tokens.extend(clean(token, recurse=recurse))
        elif token.is_group and recurse:
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

    def serialize(self, reindent=False) -> str:
        raw_string = ";".join([serialize_tokens(statement.tokens) for statement in self._parsed_statements])
        return sqlparse.format(raw_string, reindent=reindent, keyword_case='upper')

    def __parse(self) -> List[sqlparse.sql.Statement]:
        split_statements = []
        for split in sqlparse.split(self._source.source()):
            parsed_split = sqlparse.parse(split)  # sqlparse.format(split, reindent=True, keyword_case='upper'))
            for statement in parsed_split:
                serialized = serialize_tokens(statement.tokens)
                stripped_tokens = sqlparse.parse(serialized)
                split_statements.extend(stripped_tokens)
        return split_statements

    def extract_statements(self) -> List[Tuple[str, sqlparse.tokens]]:
        statements = []
        for statement in self.parsed_statements():
            sub_statements = []
            for sub_statement in extract_statements(statement.tokens):
                sub_statements.append(sub_statement)
            statements.append(sub_statements)
        return statements


class DecomposedSource:

    def __init__(self,
                 parsed_source: ParsedSource,
                 known_dependencies: Dict[str, DecomposedSource] = None,
                 extract_statements = True,
                 alias: str = None):
        self._alias = alias
        self._dependencies = []
        self._parsed_sources = []
        self._known_dependencies = known_dependencies
        if extract_statements:
            for statements in parsed_source.extract_statements():
                for name, tokens in statements:
                    sub_source = ParsedSource(Source(serialize_tokens(tokens)))
                    decomposed_dependencies = self._decompose_dependencies(
                        name,
                        sub_source,
                        top_level_statements=self._known_dependencies or statements)
                    self._dependencies.extend(decomposed_dependencies)
                    self._parsed_sources.append(sub_source)
        else:
            for statement in parsed_source.parsed_statements():
                sub_source = ParsedSource(Source(serialize_tokens(statement.tokens)))
                decomposed_dependencies = self._decompose_dependencies(
                    self._alias,
                    sub_source,
                    top_level_statements=self._known_dependencies)
                self._dependencies.extend(decomposed_dependencies)
                self._parsed_sources.append(sub_source)

    def parsed_sources(self) -> List[ParsedSource]:
        return self._parsed_sources

    def statements(self) -> List[sqlparse.tokens]:
        return self._parsed_sources.statements()

    def dependencies(self, recurse: bool = False) -> List[Dict[str, DecomposedSource]]:
        dependencies = self._dependencies
        if recurse:
            for dependency_map in dependencies:
                for name, dependency in dependency_map.items():
                    dependencies.extend(dependency.dependencies())
        return dependencies

    def alias(self) -> str:
        return self._alias

    def serialize(self, recurse: bool = False, top_level: bool = True) -> str:
        raw_string = ";".join([parsed_source.serialize() for parsed_source in self._parsed_sources])
        return sqlparse.format(raw_string, keyword_case='upper')

    def has_dependency(self, potential_dependency: DecomposedSource, recurse: bool = True) -> bool:

        ret_val = False
        alias = potential_dependency.alias()
        if alias:
            ret_val = self._has_dependency(potential_dependency)
            if recurse:
                for dependency_mapping in self._dependencies:
                    for decomposed_source in dependency_mapping.values():
                        if decomposed_source.has_dependency(potential_dependency):
                            ret_val = True
                            break
        return ret_val

    def _has_dependency(self, potential_dependency: DecomposedSource) -> bool:
        return potential_dependency.alias() and \
               next((dep for dep in self.dependencies() if potential_dependency.alias() in dep.keys()), None) is not None


    def _decompose_dependencies(self,
                                name: str,
                                parsed_source: ParsedSource,
                                top_level_statements: Dict[str, DecomposedSource]) -> List[Dict[str, DecomposedSource]]:

        all_statement_dependencies = []
        # recursively decompose statements by dependency
        for statement in parsed_source.parsed_statements():
            statement_dependencies = {}
            aliases = [statement_pair[0] for statement_pair in top_level_statements if statement_pair[0]]
            for dependency in map_dependencies(name, statement, known_aliases=aliases):
                dependency_tokens = next(statement_pair[1] for statement_pair in top_level_statements if statement_pair[0] == dependency)
                sub_source = ParsedSource(Source(serialize_tokens(dependency_tokens)))
                statement_dependencies[dependency] = DecomposedSource(
                    sub_source,
                    known_dependencies=top_level_statements,
                    alias=dependency,
                    extract_statements=False)
            all_statement_dependencies.append(statement_dependencies)
        return all_statement_dependencies


class EncodedSource:

    def __init__(self,
                 decomposed_source: DecomposedSource,
                 known_dependencies: Dict[str, EncodedSource] = None,
                 prefix: str = ""):
        assert(isinstance(decomposed_source, DecomposedSource))
        self._alias = decomposed_source.alias()
        self._decomposed_source = decomposed_source
        self._aliased_source = []
        self._hashed_sources = []
        self._encoded_sources = []
        self._encoded_dependencies = []
        self._known_dependencies = known_dependencies or {}
        for parsed_source, dependencies in zip(decomposed_source.parsed_sources(), decomposed_source.dependencies()):
            # recursively encode dependencies first
            sub_encoded_dependencies = []
            include_source_dependencies = []
            serialized = ""
            unencoded_dependencies_by_name = {}
            for alias, dependency in dependencies.items():
                if alias:
                    if dependency.alias().startswith(prefix):
                        encoded_dependency = EncodedSource(dependency, known_dependencies=self._known_dependencies, prefix=prefix)
                        sub_encoded_dependencies.append(encoded_dependency)
                        #all_encoded_dependencies[alias] = encoded_dependency
                        #include_source_dependencies.append(f"{alias} AS (SELECT * FROM `{encoded_dependency.hashed_sources()[-1]}`)")
                    else:
                        unencoded_dependencies_by_name[alias] = dependency
                        dependency_map_list = dependency.dependencies(recurse=True)
                        for dependency_map in dependency_map_list:
                            unencoded_dependencies_by_name.update(dependency_map)
                    #include_source_dependencies.extend(f"{alias} AS ({dependency.serialize(recurse=True)})")
            self._encoded_dependencies.append(sub_encoded_dependencies)

            # determine ordering of dependencies to include.
            for encoded_dependency in sub_encoded_dependencies:
                # if we have encoded, prefer that. Remove from non-encoded
                unencoded_dependencies_by_name.pop(encoded_dependency.alias(), None)
                include_source_dependencies.append(encoded_dependency)
            # now add non-encoded
            for alias, dependency in unencoded_dependencies_by_name.items():
                include_source_dependencies.append(dependency)

            # https://stackoverflow.com/questions/47192626/deceptively-simple-implementation-of-topological-sorting-in-python
            # def iterative_topological_sort(graph, start):
            #     seen = set()
            #     stack = []    # path variable is gone, stack and order are new
            #     order = []    # order will be in reverse order at first
            #     q = [start]
            #     while q:
            #         v = q.pop()
            #         if v not in seen:
            #             seen.add(v) # no need to append to path any more
            #             q.extend(graph[v])
            #
            #             while stack and v not in graph[stack[-1]]:
            #                 order.append(stack.pop())
            #             stack.append(v)
            #
            #     return stack + order[::-1]

            if include_source_dependencies:
                #logger.info(f"BEFORE include_source_dependencies:{[dep.alias() for dep in include_source_dependencies]}")
                dep_graph = Graph(len(include_source_dependencies))
                #start = [dep for dep in include_source_dependencies if dep.alias() in dependencies.keys()]
                #logger.info(f"start deps:{[dep.alias() for dep in start]}")
                idx_source = 0
                for source in include_source_dependencies:
                    if isinstance(source, EncodedSource):
                        decomposed_source = source.decomposed_source()
                    else:
                        decomposed_source = source
                    idx_dep = 0
                    #for dep in [dep for dep in include_source_dependencies if dep.alias() in source_dep_keys]:
                    for target in include_source_dependencies:
                        if decomposed_source is not target and decomposed_source.has_dependency(target):
                            #logger.info(f"adding edge:source: {source.alias()} dep: {target.alias()}")
                            dep_graph.addEdge(idx_dep, idx_source)
                        idx_dep += 1
                    idx_source += 1
                sorted_indices = dep_graph.topologicalSort()
                #logger.info(f"sorted_indices:{sorted_indices}")
                include_source_dependencies_new = [include_source_dependencies[idx] for idx in sorted_indices]
                include_source_dependencies = include_source_dependencies_new


            #logger.info(f"AFTER self:{self.alias()} include_source_dependencies:{[dep.alias() for dep in include_source_dependencies]}")

            # render out source with its dependencies
            if include_source_dependencies:
                serialized += "WITH "
                serialized += ",\n".join([f" {dep.alias()} AS ({dep.serialize()})" for dep in include_source_dependencies]) + "\n"
            serialized += f"{parsed_source.serialize()}"
            self._encoded_sources.append(serialized)
            import hashlib
            hasher = hashlib.sha1()
            hasher.update(serialized.encode('utf-8'))
            hashed = hasher.hexdigest()
            self._hashed_sources.append(hashed)
            self._known_dependencies[hashed] = self

    def alias(self) -> str:
        return self._alias

    def decomposed_source(self) -> DecomposedSource:
        return self._decomposed_source

    # still retains source unencoded, but with encoded dependency references
    def encoded_sources(self) -> List[str]:
        return self._encoded_sources

    # hashes all sources, included encoded dependency references
    def hashed_sources(self) -> List[str]:
        return self._hashed_sources

    # direct encoded dependencies
    def encoded_dependencies(self) -> List[List[EncodedSource]]:
        return self._encoded_dependencies

    # all encoded sources known by this source structure
    def all_encoded_sources_by_name(self) -> Dict[str, EncodedSource]:
        return self._known_dependencies

    def serialize(self, reindent=False) -> str:
        return sqlparse.format(f"SELECT * FROM `{self._hashed_sources[-1]}`", reindent=reindent, keyword_case='upper')

    @staticmethod
    def from_str(source_str: str, prefix=""):
        return EncodedSource(DecomposedSource(ParsedSource(Source(source_str))), prefix=prefix)


def map_dependencies(name: str, statement: sqlparse.sql.Statement, known_aliases: List[str]) -> List[str]:
    single_dependencies = map_dependencies_single(name=name, known_aliases=known_aliases, tokens=statement.tokens)
    return single_dependencies


def map_dependencies_single(name: str, known_aliases: List[str], tokens: sqlparse.tokens) -> List[str]:
    dependency_list = []
    for token in tokens:
        # TODO: might need recursive flatten here
        for flat_token in token.flatten():
            dependency = flat_token.value
            # see if we have a query which maps to this name
            if (not name or dependency != name) and dependency in known_aliases:
                dependency_list.append(dependency)
        # dependencies[cte_name] = dependency_list
    return dependency_list


def extract_statements(tokens: sqlparse.tokens) -> Union[str, sqlparse.tokens]:
    remaining_tokens = []
    found_with = False
    expect_comma = False
    encountered_non_whitespace = False
    for token in tokens:
        # from https://www.programcreek.com/python/?code=dbcli%2Flitecli%2Flitecli-master%2Flitecli%2Fpackages%2Fparseutils.py
        if found_with and not expect_comma and (isinstance(token, sqlparse.sql.IdentifierList) or isinstance(token, sqlparse.sql.Identifier)):
            item_list = token.get_identifiers() if isinstance(token, sqlparse.sql.IdentifierList) else [token]
            for identifier in item_list:
                # Sometimes Keywords (such as FROM ) are classified as
                # identifiers which don't have the get_real_name() method.
                try:
                    real_name = identifier.get_real_name()
                except AttributeError:
                    continue
                # we are starting a new identifier. return what we have so far and clear it for after the id
                if remaining_tokens:
                    yield None, remaining_tokens
                    remaining_tokens = []
                cte_tokens = identifier.tokens

                # yield real_name, identifier

                found_as = False
                for cte_token in cte_tokens:
                    # are we defining a CTE identifier?
                    if found_as:
                        if type(cte_token) == sqlparse.sql.Parenthesis:
                            found_as = False
                            expect_comma = True
                            # get everything between parens, the identifiers internals, to replace
                            # return everything up to, including the opening paren, but but not including
                            # the identifier internals, to replace
                            between_parens = list(cte_token.tokens)[1:-1]
                            remaining_tokens.append(between_parens)
                            # now add everything after the internals, including trailing paren, and continue
                            yield real_name, remaining_tokens
                            remaining_tokens = []
                            break  # stop extracting this cte
                    if not found_as and cte_token.value.upper() == 'AS':
                        found_as = True


        else:
            # if we are expecting a comma and see one, we expect another cte
            # if expect_comma and token.value == ",":
            #     expect_comma = False
            # el
            if token.value.upper() == "WITH":
                found_with = True
            elif not token.is_whitespace or encountered_non_whitespace:
                encountered_non_whitespace = True
                remaining_tokens.append(token)

    if remaining_tokens:
        yield None, remaining_tokens



# Class to represent a graph
from collections import defaultdict
class Graph:
    def __init__(self, vertices):
        self.graph = defaultdict(list)  # dictionary containing adjacency List
        self.V = vertices  # No. of vertices

    # function to add an edge to graph
    def addEdge(self, u, v):
        self.graph[u].append(v)

        # A recursive function used by topologicalSort
    def topologicalSortUtil(self, v, visited, stack):

        # Mark the current node as visited.
        visited[v] = True

        # Recur for all the vertices adjacent to this vertex
        for i in self.graph[v]:
            if visited[i] == False:
                self.topologicalSortUtil(i, visited, stack)

        # Push current vertex to stack which stores result
        stack.append(v)

        # The function to do Topological Sort. It uses recursive
    # topologicalSortUtil()
    def topologicalSort(self):
        # Mark all the vertices as not visited
        visited = [False]*self.V
        stack = []

        # Call the recursive helper function to store Topological
        # Sort starting from all vertices one by one
        for i in range(self.V):
            if visited[i] == False:
                self.topologicalSortUtil(i, visited, stack)

                # Print contents of the stack
        return stack[::-1]  # return list in reverse order

# def sort_by_dependence(x, y):
#     if isinstance(x, EncodedSource):
#         x = x.decomposed_source()
#     if isinstance(y, EncodedSource):
#         y = y.decomposed_source()
#     # x a dependency of y?
#     ret_val = 0
#     if y.has_dependency(x, recurse=True):
#         ret_val = -1
#     elif x.has_dependency(y, recurse=True):
#         ret_val = 1
#     logger.info("--")
#     logger.info(f"{ret_val} x.alias(): {x.alias()} x:{x} y.alias():{y.alias()} y:{y}")
#     return ret_val
# include_source_dependencies.sort(key=functools.cmp_to_key(sort_by_dependence))
# logger.info(f"dependency order:{','.join([dep.alias() for dep in include_source_dependencies])}")




