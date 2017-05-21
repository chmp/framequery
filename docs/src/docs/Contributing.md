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

The documentation is auto-generated from markdown files extended with a subset
of sphinx directives. The input files can be found in `docs/src/`, which can be
built with the `docs/update_docs.py` script. When updating the docs follow 
these steps:

1. Update the documentation sources in `docs/src`
2. Generate the output with `python docs/update_docs.py`
3. Commit both source and output files
