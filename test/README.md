# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). The tests generate their own data; no benchmark downloads or absolute paths are required. See [the development guide](../docs/development.md) for the DuckDB 1.5.6 build and patch setup.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```