import csv
from datetime import datetime
from pathlib import Path
from typing import List, Optional, NamedTuple, Tuple
from urllib.parse import ParseResult as URLParseResult

import click

from sparql_query_tools.ParseSparqlJsonResult import SparqlJsonResultStats
from sparql_query_tools.RunQuery import run_queries, QueryResult
from sparql_query_tools.clickextension.IntList import IntList
from sparql_query_tools.clickextension.QueryFilesParam import QueriesFileParam
from sparql_query_tools.clickextension.URLParam import URLParam
from sparql_query_tools.commons import parse_queries_file


class OutputCSVRows(NamedTuple):
    format: str
    dataset: str
    triplestore: str
    queryID: int
    qps: float
    succeeded: bool
    failed: bool
    httpCode: int
    errorMessage: str
    time: float
    contentLength: int
    parsingSucceeded: bool
    numberOfVariables: Optional[int]
    numberOfSolutions: Optional[int]
    numberOfBindings: Optional[int]
    resultParsingTime: Optional[float]
    parsingErrorMessage: Optional[str]


@click.command()
@click.option('--url', '-u', nargs=1, type=URLParam(), help='URL with port of the SPARQL endpoint.')
@click.option('--queries', '-q', nargs=1, type=QueriesFileParam(), help='A file containing one sparql query per line')
@click.option('--include', '-i', required=False, cls=IntList,
              help='Run only these queries. Queries are numbered 0, 1, 2 ... in the order they are stored in the file.')
@click.option('--exclude', '-e', required=False, cls=IntList, help="Don't run these queries.")
@click.option('--datasetname', '-dn', required=False, type=str, default=None,
              help='Name of the dataset loaded in the store.')
@click.option('--storename', '-sn', required=False, type=str, default=None, help='Name of the store being benched.')
@click.option('--save/--dont-save', ' /-d', required=False, type=bool, default=True,
              help="Don't write the results to disk.")
@click.option('--save-only-error', '-sor', is_flag=True, required=False, type=bool, default=False,
              help="Save the sparql result file only in case of an error.")
@click.option('--dont-parse', '-dp', is_flag=True, required=False, type=bool, default=False,
              help="Don't parse the result. HTTP Content-Length is still reported.")
@click.option('--time-out', '-t', required=False, type=int, default=600000,
              help="Timeout in milliseconds. Default is 10 minutes.")
@click.option('--output', '-o', required=False, type=str, default=None,
              help='Custom location for output csv file.'
                   'If set the result files are written in a directory next to the csv file with the same name.')
@click.option('--verbose', '-v', is_flag=True, required=False, type=bool, default=False, help='verbose logging')
def cli(url: URLParseResult, queries: Path, include: Optional[List[int]], exclude: Optional[List[int]],
        storename: Optional[str], datasetname: Optional[str], save: bool, save_only_error: bool, dont_parse: bool,
        time_out: int,
        output: Optional[str], verbose: bool):
    if output is not None:
        if Path(output).suffix != ".csv":
            click.echo("Output file extension must be .csv")
            exit(1)
        click.echo("Output file is set to {}".format(str(Path(output).absolute())))

    if output is None:
        output_name: str = "HTTP_{}_{}_{}".format(
            datasetname if datasetname is not None else "unspecified-dataset",
            storename if storename is not None else "unspecified-store",
            datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
        output_dir = Path.cwd().joinpath(output_name)
        result_files_dir = output_dir.joinpath("result_files")
    else:
        output_name: str = Path(output).stem
        output_dir = Path(output).parent
        result_files_dir = output_dir.joinpath(Path(output_name).name)

    output_dir.mkdir(parents=True, exist_ok=True)
    result_files_dir.mkdir(parents=True, exist_ok=True)

    query_ids, queries = parse_queries_file(queries, include, exclude)

    csv_path: Path = output_dir.joinpath(output_name + ".csv")

    results: List[Tuple[int, str, QueryResult, Optional[SparqlJsonResultStats]]] = list()
    if save:
        if not save_only_error:
            results = [*run_queries(query_ids, queries, url, result_files_dir, parse=not dont_parse, timeout_ms=time_out, verbose=verbose)]
        else:
            for result in run_queries(query_ids, queries, url, result_files_dir, parse=not dont_parse, timeout_ms=time_out, verbose=verbose):
                results.append(result)
                query_id, query, download_result, parse_result = result
                if download_result.status == 200 or (
                        download_result.status == 200 and not dont_parse and parse_result.success):
                    if download_result.path.exists():
                        download_result.path.unlink()
    else:
        if dont_parse:
            results = [*run_queries(query_ids, queries, url, None, parse=False, timeout_ms=time_out, verbose=verbose)]
        else:
            for result in run_queries(query_ids, queries, url, result_files_dir, parse=True, timeout_ms=time_out, verbose=verbose):
                results.append(result)
                query_id, query, download_result, parse_result = result
                if download_result.path.exists():
                    download_result.path.unlink()

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=OutputCSVRows._fields)
        writer.writeheader()
        for query_id, query, download_result, parse_result in results:
            http_succeeded = download_result.status == 200 and download_result.error_message is None

            row = OutputCSVRows(
                format="HTTP",
                dataset=datasetname,
                triplestore=storename,
                queryID=query_id,
                qps=1.0 / download_result.duration_s if http_succeeded else 0.0,
                succeeded=http_succeeded,
                failed=not http_succeeded,
                httpCode=download_result.status,
                errorMessage=download_result.error_message,
                time=download_result.duration_s,
                contentLength=download_result.file_size_bytes,
                parsingSucceeded=parse_result.success if http_succeeded and parse_result else None,
                numberOfVariables=parse_result.no_of_variables if http_succeeded and parse_result else None,
                numberOfSolutions=parse_result.no_of_solutions if http_succeeded and parse_result else None,
                numberOfBindings=parse_result.no_of_var_bindings if http_succeeded and parse_result else None,
                resultParsingTime=parse_result.parse_duration if http_succeeded and parse_result else None,
                parsingErrorMessage=parse_result.error_message if http_succeeded and parse_result else None,
            )
            writer.writerow(row._asdict())
    if not any(result_files_dir.iterdir()):
        result_files_dir.rmdir()


if __name__ == '__main__':
    cli()
