# Mixed Pipeline

Pipeline combining shell, Python, and SQL runner types with a SQL check.

Dependency chain: `fetch_api_data` (shell) -> `clean_api_data` (python) -> `stg_api_events` (sql).

After `stg_api_events` materializes, a `sql_check` (`row_count`) runs automatically to verify the table is non-empty. Check type is inferred from the `.sql` extension.

## Run

```bash
granicus run pipeline.yaml
```
