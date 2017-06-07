# framequery API

## framequery

###  framequery.execute
`framequery.execute(q, scope, model, scope=None, model='pandas', basepath='.')`

Execute queries against the provided scope.

#### Parameters

* **scope** (*dict*):
  a mapping of table names to dataframes. If not provided the globals and
  locals of the calling scope are used.
* **model** (*Union[str,Model]*):
  the datamodel to use. Currently `"pandas"` and `"dask"` are
  supported as string values. For better customization create the model
  instances independently and pass them as arguments.
  
  See [framequery.PandasModel](#framequerypandasmodel) and [framequery.DaskModel](#framequerydaskmodel)
  for further information.
* **basepath** (*str*):
  the basepath of `copy from` and `copy to` operations. This argument
  is only when constructing the models. For independently constructed
  models, the basepath can be set via their `__init__` arguments.



###  framequery.Executor
`framequery.Executor(scope, model, basepath, scope=None, model='pandas', basepath='.')`

A persistent executor - to allow reusing scopes and models.

#### Parameters

* **scope** (*any*):
  a mapping of table-names to dataframes. If not given, an empty scope
  is created.
* **model** (*any*):
  the model to use, see [framequery.execute](#framequeryexecute).
* **basepath** (*str*):
  the basepath of the model.



###  framequery.Executor.add_lateral_function
`framequery.Executor.add_lateral_function(name, meta=None)`

Add a table-function that supports lateral joins.

#### Parameters

* **name** (*str*):
  the name of the function.
* **func** (*callable*):
  the function. It should take any number of positional arguments and
  return a dataframe.
* **meta** (*Optional[List[Tuple[str,type]*):
  an optional meta data list of name-type-pairs. The dask excecutor
  requires meta data information to handle lateral joins.



###  framequery.DaskModel
`framequery.DaskModel(**kwargs)`

A framequery model for `dask.dataframe.DataFrame` objects.

For a list of keyword arguments see [framequery.PandasModel](#framequerypandasmodel).

The dask executor supports scopes with both pandas and dask dataframes.
The former will be converted into later automatically, as needed.



###  framequery.PandasModel
`framequery.PandasModel(basepath, strict, basepath='.', strict=False)`

A framequery model for `pandas.DataFrame` objects.

#### Parameters

* **basepath** (*str*):
  the path relative to which any `copy from` and `copy to` statements
  are interpreted.
* **strict** (*bool*):
  if True, mimic SQL behavior in group-by and join.



## framequery.alchemy

###  framequery.alchemy.get_executor
`framequery.alchemy.get_executor()`

Extract the executor from a framequery sqlalchemy engine or connection.

Usage:

```
executor = get_executor(engine)
```



## framequery.parser

###  framequery.parser.parse
`framequery.parser.parse(query, what=None)`

Parse a query into an `framequery.ast` object.

#### Parameters

* **query** (*str*):
  the query to parse

#### Returns

{body}



###  framequery.parser.tokenize
`framequery.parser.tokenize()`

Tokenize the query string.



##  framequery.parser.ast

Module of ast classes.



###  framequery.parser.ast.Select
` framequery.parser.ast.Select(columns, from_clause, where_clause, group_by_clause, having_clause, order_by_clause, limit_clause, quantifier, cte)`

The ast node of a select statement.

#### Instance variables

* **columns** (*Sequence[Column]*):
  the list of selected columns
* **cte** (*Sequence[SubQuery]*):
  a list of comment tables, each given a
  [framequery.parser.ast.SubQuery](#framequeryparserastsubquery).



###  framequery.parser.ast.SubQuery
` framequery.parser.ast.SubQuery(query, alias)`

A subquery or CTE.

#### Instance variables

* **query** (*Select*):

* **alias** (*Name*):


