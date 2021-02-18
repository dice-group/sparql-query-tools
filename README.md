# SPARQL Query Tools

Currently, SPARQL query tools include exactly one tool: `sparql_result_analysis`

More will be integrated later so that the plural in the name will make sense. 😉

## Tools

- `sparql_result_analysis`: Queries a triple store and provides info and metrics on the results. Creates a folder with the resulting json files, and a CSV file containing the infos and metrics about the querying and results. 

## Install

From git repository:
```
git clone https://github.com/dice-group/sparql-query-tools.git
cd sparql-query-tools
pip install .
```

or with pip from PyPI:
```
pip install sparql-query-tools
```
