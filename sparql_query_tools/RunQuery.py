import time
from pathlib import Path
from typing import NamedTuple, Optional, List, Iterator, Tuple
from urllib.parse import ParseResult as URLParseResult

import click
import pycurl

from sparql_query_tools.ParseSparqlJsonResult import SparqlJsonResultStats, parse_sparql_json_result


class QueryResult(NamedTuple):
    path: Path
    file_size_bytes: Optional[int]
    duration_s: float
    status: int
    error_message: Optional[str]


def run_query(endpoint: URLParseResult, query: str, query_id: int, output_dir: Path) -> QueryResult:
    from urllib.parse import parse_qs, urlencode
    query_params: dict = parse_qs(endpoint.query)
    query_params["query"] = query
    query_url = URLParseResult(scheme=endpoint.scheme,
                               netloc=endpoint.netloc,
                               path=endpoint.path,
                               params=endpoint.params,
                               query=urlencode(query_params),
                               fragment=endpoint.fragment).geturl()
    file: Path = output_dir.joinpath('result_q{:05d}.json'.format(query_id))
    status = -1
    duration = time.time()
    with open(file, 'wb') as f:
        c = pycurl.Curl()
        c.setopt(c.URL, query_url)
        c.setopt(pycurl.HTTPHEADER, ["Content-Type:application/sparql-results+json"])
        c.setopt(pycurl.HTTPGET, 1)
        c.setopt(c.WRITEDATA, f)
        try:
            c.perform()
        except pycurl.error as e:
            status = c.getinfo(c.RESPONSE_CODE)
            c.close()
            duration = time.time() - duration
            return QueryResult(file, None, duration, status, str(e))
        status = c.getinfo(c.RESPONSE_CODE)
        c.close()
    duration = time.time() - duration
    file_size_bytes = Path(file).stat().st_size

    return QueryResult(file, file_size_bytes, duration, status, "")


def prettify_query(query: str) -> str:
    pretty_query = query.split("SELECT")
    pretty_query = "SELECT " + pretty_query[1] if len(pretty_query) > 1 else pretty_query[0]
    # replace whitespaces with a single space
    import re
    pretty_query = re.sub(r"[\s]+", " ", str(pretty_query))
    return pretty_query


def run_queries(query_ids: List[int], queries: List[str], endpoint, result_files_dir: Path) -> Iterator[Tuple[
    int, str, QueryResult, Optional[SparqlJsonResultStats]]]:
    for query_id, query in zip(query_ids, queries):
        click.echo("\nQuery: {}\n"
                   "Query ID: {}".format(prettify_query(query), query_id))
        download_result = run_query(endpoint, query, query_id, result_files_dir)
        file, file_size_bytes, retrieval_duration, status, error_message = download_result
        click.echo("Retrieval duration: {} s\n"
                   "Result size: {} bytes".format(retrieval_duration, file_size_bytes))
        click.echo("{}".format(status))
        # if status == 200:
        parse_result = parse_sparql_json_result(file)
        no_of_variables, no_of_solutions, no_of_var_bindings, parse_duration, parse_success, _ = parse_result
        if parse_success:
            click.echo("Parsing duration: {} s\n"
                       "Number of variables: {}\n"
                       "Number of solutions: {}\n"
                       "Number of variable bindings: {}"
                       .format(parse_duration, no_of_variables, no_of_solutions, no_of_var_bindings))
        yield query_id, query, download_result, parse_result
