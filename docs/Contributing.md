# Development

**TODO: add framequery internal explanation**

## Running tests

First install `tox` via `pip intall tox`. Then, execute:

```bash
tox
```

To run conformance tests against a database, set the environment variable 
`FQ_TEST_DB` to sqlalchemy connection string:

```bash
export FQ_TEST_DB=postgresql://postgres@localhost:5432/postgres
tox
```

## Writing documentation

All documentation