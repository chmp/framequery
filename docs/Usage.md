# framequery Usage

Framequery can both be used on [its own](#standalone-usage) and in combination
with [sqlalchemy](#sqlalchemy-support). Framequery also supports both pure 
pandas and dask dataframes.

## Standalone usage

To execute queries against dataframes in the current scope, use the 
[`framequery.execute`](API.md#framequeryexecute) function:

```python
import framequery as fq

df = pd.read_csv('test.csv')
result_df = fq.execute('select * from df')
```

To switch between pandas and dask modes, pass either `model='pandas'` or 
`model='dask'`. The dask model can query both pandas and dask dataframes, but
requires extra dependencies and introduces additional overhead for simple 
workloads. Per default framequery uses the pandas only model, to enable the
dask model use:
 
```python
result_df = fq.execute('select * from df', model='dask')
```

While framequery queries the surrounding scope per default, the scope can also
be passed explicitly as a dict mapping table names to dataframes. For example:
 
```python
scope = {'table': df}
 
result_df = fq.execute('select * from table', scope=scope)
```

It is possible to further customize execution by creating a dedicated 
[`framequery.Executor`](API.md#framequeryexecutor) object:

```python
executor = fq.Executor()
result_df = executor.execute('select * from table')
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
engine = create_engine('framequery:///?model=dask')
```

**TODO describe spec files, once stable**

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
