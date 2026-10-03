# dbt data tests (SQL · dbt track)

Phase 4 adds the project's own dbt data tests here, next to the generic tests declared in the models' `.yml` files:
- `generic/`: reusable tests with arguments (`{% test name(model, column_name, ...) %}`), used from the `.yml` files;
- `singular/`: one `.sql` file per rule; the test fails when the query returns rows.

Every test cites the rule it checks (`contract/transform-spec.md` §, or a contract check id) and has a severity:
`error` blocks the publish, `warn` is reported. Run them with `dbt build` (or `make test-sql`).
