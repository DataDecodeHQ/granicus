# BigQuery Pipeline

SQL transform pipeline with three layers: staging, intermediate, entity.

Assets form a linear dependency chain: `stg_orders` -> `int_order_summary` -> `entity_customers`. Dependencies are declared in granicus directive blocks within each SQL file.

Demonstrates partitioning (`stg_orders` by day), clustering (`entity_customers` by customer_id), grain declaration, and scheduled execution.

## Run

```bash
granicus run pipeline.yaml
```
