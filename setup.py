from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()
#
with open('LICENSE') as f:
    license = f.read()

setup(
    name='sparql-query-tools',
    version='0.1.0',
    description=readme,
    description_content_type="text/markdown",
    long_description=readme,
    long_description_content_type="text/markdown",
    author='Alexander Bigerl',
    author_email='info@dice-research.org',
    url='https://github.com/dice-group/sparql-query-tools',
    license="AGPL v3",
    packages=find_packages(),
    include_package_data=True,
classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)", ],
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
