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


def run_query(endpoint: URLParseResult, query: str, query_id: int, output_dir: Optional[Path] = None, timeout_ms: int = 600000,
              verbose: bool = False) -> QueryResult:
    from urllib.parse import parse_qs, urlencode
    query_params: dict = parse_qs(endpoint.query)
    query_params = {k: v[0] if type(v) is list and len(v) == 1 else v for k, v in query_params.items()}
    query_params["query"] = query
    query_url = URLParseResult(scheme=endpoint.scheme,
                               netloc=endpoint.netloc,
                               path=endpoint.path,
                               params=endpoint.params,
                               query=urlencode(query_params),
                               fragment=endpoint.fragment).geturl()
    if verbose:
        click.echo(f"Request URL: {query_url}")
    file: Optional[Path] = output_dir.joinpath(f'result_q{query_id:05d}.json') \
        if output_dir is not None \
        else None
    status = None
    duration = time.time()
    c = pycurl.Curl()
    c.setopt(c.URL, query_url)
    c.setopt(pycurl.HTTPHEADER, ["Accept:application/sparql-results+json"])
    c.setopt(pycurl.HTTPGET, 1)
    c.setopt(pycurl.TIMEOUT_MS, timeout_ms)

    from io import BytesIO
    if file is None:
        buffer = BytesIO()
    try:
        if file is None:
            c.setopt(c.WRITEDATA, buffer)
            c.perform()
        else:
            with open(file, 'wb') as f:
                c.setopt(c.WRITEDATA, f)
                c.perform()
    except pycurl.error as e:
        status = c.getinfo(c.RESPONSE_CODE)
        if status == 0:
            status = None
        c.close()
        duration = time.time() - duration
        return QueryResult(file, None, duration, status, " | ".join(str(i) for i in e.args))
    status = c.getinfo(c.RESPONSE_CODE)
    c.close()
    duration = time.time() - duration  #

    file_size_bytes = Path(file).stat().st_size \
        if file is not None \
        else len(buffer.getvalue())

    return QueryResult(file, file_size_bytes, duration, status, None)


def prettify_query(query: str) -> str:
    pretty_query = query.split("SELECT")
    pretty_query = "SELECT " + pretty_query[1] if len(pretty_query) > 1 else pretty_query[0]
    # replace whitespaces with a single space
    import re
    pretty_query = re.sub(r"[\s]+", " ", str(pretty_query))
    return pretty_query


def run_queries(query_ids: List[int], queries: List[str], endpoint, result_files_dir: Optional[Path],
                parse: bool = True, timeout_ms: int = 600000, verbose: bool = True) -> Iterator[Tuple[
    int, str, QueryResult, Optional[SparqlJsonResultStats]]]:
    for query_id, query in zip(query_ids, queries):
        if verbose:
            click.echo("\nQuery: {}\n".format(prettify_query(query)) +
                       f"Query ID: {query_id}")
        download_result = run_query(endpoint, query, query_id, result_files_dir, timeout_ms=timeout_ms, verbose=verbose)

        file, file_size_bytes, retrieval_duration, status, error_message = download_result
        if verbose:
            click.echo("Retrieval duration: {} s\n"
                       "Result size: {} bytes".format(retrieval_duration, file_size_bytes))
            click.echo("{}".format(status))
        if status == 200 and error_message is None:
            if parse:
                parse_result = parse_sparql_json_result(file)
                no_of_variables, no_of_solutions, no_of_var_bindings, parse_duration, parse_success, _ = parse_result
                if parse_success:
                    if verbose:
                        click.echo(
                            f"""Parsing duration: {parse_duration} s
Number of variables: {no_of_variables}
Number of solutions: {no_of_solutions}
Number of variable bindings: {no_of_var_bindings}""")
                yield query_id, query, download_result, parse_result
            else:
                yield query_id, query, download_result, None
        else:
            yield query_id, query, download_result, None
