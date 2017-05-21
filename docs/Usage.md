# framequery Usage

## Standalone usage

```python
from framequery import execute
```

## sqlalchemy support

framequery ships with its own sqlalchemy dialect. To create a framequery engine
simply use the `framequery:///` url:
 
```python
from sqlalchemy import create_engine
engine  = create_engine('framequery:///')
```
 
Additional parameters, such as `basepath` and `model` can be passed in the 
query string. For example, use  `framequery:///?model=dask` to create a 
framequery engine with `dask` support:

```python
engine = create_engine(
```

To access the executor of an engine, use the 
[`framequery.alchemy.get_executor`](API.md#framequeryalchemyget_executor) 
function. The returned executor can then be used to add custom functions:

```python
from framequery.alchemy import get_executor

executor = get_executor(engine)
executor.add_function('foo', lambda: 'foo')
```

Or, to add dataframes manually to the scope:

```python
executor = get_executor(engine)
executor.update(new_table=df)
```

See the [`framquery.Executor`](API.md#framequeryexecutor) docs for a detailed
description of availabel functionality.

The constructed engine supports both being queries via text and sqlalchemy
objects: 

```python
engine.execute('select sum(c1) from table').fetchall()
engine.execute(select([func.count(table.c.c1)]).fetchall()
```

## Dask vs. pandas models

Mention that the dask model can work with pandas dataframes. Explain the 
different performance characteristics, due to the distribution overhead 
involved and how the dask model is per default lazy and may recompute certain
values.