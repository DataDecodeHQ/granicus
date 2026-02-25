# Hello World Pipeline

Minimal shell-based pipeline demonstrating extract-transform-load dependencies.

Three shell assets run sequentially: `extract` -> `transform` -> `load`. Each writes to `/tmp/hello_world/` so the next step can pick up the output.

## Run

```bash
granicus run pipeline.yaml
```
