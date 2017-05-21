# framequery API

##  framequery.execute
` framequery.execute(q, scope, model, scope=None, model='pandas', basepath='.')`

Execute queries against the provided scope.

**param dict scope** a mapping of table names to dataframes. If not provided the globals and
locals of the calling scope are used.

**param Union[str,Model] model** the datamodel to use. Currently `"pandas"` and `"dask"` are
supported as string values. For better customization create the model
instances independently and pass them as arguments.

See [framequery.PandasModel](#framequerypandasmodel) and [framequery.DaskModel](#framequerydaskmodel)
for further information.

**param str basepath** the basepath of `copy from` and `copy to` operations. This argument
is only when constructing the models. For independently constructed
models, the basepath can be set via their `__init__` arguments.



##  framequery.Executor
` framequery.Executor(scope, model, basepath, scope=None, model='pandas', basepath='.')`

A persistent executor - to allow reusing scopes and models.

**param scope** a mapping of table-names to dataframes. If not given, an empty scope
is created.

**param model** the model to use, see [framequery.execute](#framequeryexecute).

**param str basepath** the basepath of the model.



##  framequery.DaskModel
` framequery.DaskModel(**kwargs)`

A framequery model for `dask.dataframe.DataFrame` objects.

For a list of keyword arguments see [framequery.PandasModel](#framequerypandasmodel).

The dask executor supports scopes with both pandas and dask dataframes.
The former will be converted into later automatically, as needed.



##  framequery.PandasModel
` framequery.PandasModel(basepath, strict, basepath='.', strict=False)`

A framequery model for `pandas.DataFrame` objects.

**param str basepath** the path relative to which any `copy from` and `copy to` statements
are interpreted.

**param bool strict** if True, mimic SQL behavior in group-by and join.



##  framequery.parser.parse
` framequery.parser.parse(query, what=None)`

Parse a query into an `framequery.ast` object.

**param str query** the query to parse

**returns** an AST object or raises an exception if the query could not be parsed.



##  framequery.parser.tokenize
` framequery.parser.tokenize()`

Tokenize the query string.



##  framequery.parser.ast

Module of ast classes.



##  framequery.alchemy.get_executor
` framequery.alchemy.get_executor()`

Extract the executor from a framequery sqlalchemy engine.

