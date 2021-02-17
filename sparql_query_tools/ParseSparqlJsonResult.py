import time
from pathlib import Path
from typing import NamedTuple, Optional


class SparqlJsonResultStats(NamedTuple):
    no_of_variables: Optional[int]
    no_of_solutions: Optional[int]
    no_of_var_bindings: Optional[int]
    parse_duration: Optional[float]
    success: Optional[bool]
    error_message: Optional[str]


def parse_sparql_json_result(file: Path) -> SparqlJsonResultStats:
    with open(file, 'rb') as f:
        duration = time.time()
        try:
            import rdflib
            parsed_result: rdflib.query.Result = rdflib.query.Result.parse(f, "json")
        except Exception as e:
            duration = time.time() - duration
            return SparqlJsonResultStats(None, None, None, duration, False, str(e))
        duration = time.time() - duration
        no_of_variables = len(parsed_result.vars)
        no_of_solutions = len(parsed_result)
        no_of_var_bindings = 0
        for solution in parsed_result:
            for binding in solution:
                if binding is not None:
                    no_of_var_bindings += 1
        return SparqlJsonResultStats(no_of_variables, no_of_solutions, no_of_var_bindings, duration, True, None)
