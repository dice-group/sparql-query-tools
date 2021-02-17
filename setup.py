from pkg_resources import parse_requirements
from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='sparql-query-tools',
    version='0.1.0',
    description='Tools to get infos about SPARQL queries.',
    long_description=readme,
    author='Alexander Bigerl',
    author_email='info@dice-research.org',
    url='https://github.com/dice-group/sparql-query-tools',
    license=license,
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "rdflib>=5.0.0",
        "click>=7.1.2",
        "pycurl>=7.43.0.6"
    ],
    entry_points='''
        [console_scripts]
        sparql_result_analysis=sparql_query_tools.sparql_result_analysis.run:cli
    '''
)
