[![Build Status](https://api.travis-ci.org/chmp/framequery.svg?branch=master)](https://travis-ci.org/chmp/framequery)

# framequery - SQL on dataframes 

framequery allows to query dataframes with SQL. Currently it targets both 
[pandas][] and [dask][], while aiming for [PostgreSQL][postgres] compatibility.
framequery is also integrated with [sqlalchemy][].

[dask]: dask.pydata.org
[pandas]: pandas.pydata.org
[postgres]: https://www.postgresql.org/
[sqlalchemy]: http://www.sqlalchemy.org/

## Getting started

Install framequery with `pip install framequery` and use `framequery.execute` 
to run queries against dataframes in your scope:

.. literalinclude:: ../../examples/readme_example.py

For a details usage see the [usage guide](docs/Usage.md) and the 
[API reference](docs/API.md).

## Changelog

.. literalinclude:: ../../Changes.md

## License

.. literalinclude:: ../../License.md
