import errno
from pathlib import Path
from typing import List, Optional, Tuple

import click


def parse_queries_file(queries_file: Path,
                       include_lines: Optional[List[int]],
                       exclude_lines: Optional[List[int]]) -> Tuple[List[int], List[str]]:
    # parse include_lines
    if include_lines is not None:
        include_lines = set(include_lines)
    # parse excludes
    if exclude_lines is not None:
        exclude_lines = set(include_lines)
    # load queries
    queries: List[str] = list()
    try:
        queries_file = open(queries_file, "r")
        queries = [query.strip() for query in queries_file.readlines()]
    except IOError as ex:
        click.echo("Could not read file: {}".format(ex), err=True)
        exit(errno.EINVAL)
    else:
        queries_file.close()
    # calculate which lines should actually be run
    # remove emtpy lines
    query_ids = {i for i in range(len(queries))}
    # allow only selected lines
    if include_lines: query_ids.intersection_update(include_lines)
    # exclude excluded lines
    if exclude_lines: query_ids.difference_update(exclude_lines)
    # sort
    query_ids = sorted(query_ids)
    import operator
    queries = operator.itemgetter(*query_ids)(queries)
    return query_ids, queries
