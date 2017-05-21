# framequery API

##  framequery.execute

Execute queries against the provided scope.

**param dict scope** a mapping of table names to dataframes. If not provided the globals and
locals of the calling scope are used.

**param Union[str,Model] model** the datamodel to use. Currently `"pandas"` and `"dask"` are
supported as string values. For better customization create the model
instances independently and pass them as arguments.

See [framequery.PandasModel](#framequery.PandasModel) and [framequery.DaskModel](#framequery.DaskModel)
for further information.

**param str basepath** the basepath of `copy from` and `copy to` operations. This argument
is only when constructing the models. For independently constructed
models, the basepath can be set via their `__init__` arguments.



##  framequery.Executor

A persistent executor - to allow reusing scopes and models.

**param scope** a mapping of table-names to dataframes. If not given, an empty scope
is created.

**param model** the model to use, see [framequery.execute](#framequery.execute).

**param str basepath** the basepath of the model.



##  framequery.DaskModel

<undocumented>



##  framequery.PandasModel

<undocumented>



##  framequery.parser.parse

Parse a query into an `framequery.ast` object.

**param str query** the query to parse

**returns** an AST object or raises an exception if the query could not be parsed.



##  framequery.parser.tokenize

Tokenize the query string.



##  framequery.parser.ast

<undocumented>

