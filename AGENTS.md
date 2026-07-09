# Agent instructions for this repository

This is a Dagster + dlt data platform. Data sources live in self-contained folders under `src/datasources/defs/` and are autoloaded — never edit `src/datasources/definitions.py` or register anything centrally. Data lands in MinIO (S3-compatible; the S3 API and `s3://` URLs, not AWS).

Human-oriented docs live in `docs/` — [developer-walkthrough.md](docs/developer-walkthrough.md) is the full tutorial; this file is the condensed, agent-oriented version.

## Environment

- Dependency manager is **uv**: `uv sync` to install, `uv add <pkg>` to add a dependency (never edit `uv.lock` by hand, never use pip).
- Run every project command through `uv run` (e.g. `uv run dg check defs`) — don't assume an activated venv.
- Destination config: `.dlt/config.toml` (bucket URL, committed) and `.dlt/secrets.toml` (credentials + MinIO `endpoint_url`, gitignored; bootstrap it with `cp .dlt/secrets.example.toml .dlt/secrets.toml`). Never write credentials anywhere else; never commit `.dlt/secrets.toml`.

## Adding a data source

1. **Probe the API first** (curl) and determine: where records sit in the response (`data_selector`), the pagination scheme (paginator type), and whether a date filter exists (enables incremental loading).
2. **Scaffold** — do not create the folder by hand:
   ```bash
   uv run dg scaffold defs datasources.components.DltLoadSourceCollection <source_name>
   ```
   `<source_name>` is short snake_case; it becomes the asset key prefix, group, and pipeline name. This generates `loader.py` (edit this) and `defs.yaml` (pre-filled; usually needs no edits).
3. **Edit `loader.py`.** Conventions the wiring depends on:
   - Expose module-level objects named exactly `source` (the dlt source) and `pipeline` (the dlt pipeline) — `defs.yaml` references `.loader.source` / `.loader.pipeline`.
   - `pipeline_name` = folder name; `dataset_name` = `<folder>_raw` (they must differ).
   - Keep `max_table_nesting=0` unless child tables are explicitly wanted.
   - Write a meaningful module docstring: its first line becomes the asset's summary in the Dagster UI, and the whole file is rendered as the asset description. Keep the file free of tutorial comments — the docs cover concepts.
4. **Validate and test-run** (required before claiming success):
   ```bash
   uv run dg check defs
   DESTINATION__FILESYSTEM__BUCKET_URL="file:///tmp/dlt-test" \
     uv run dg launch --assets "<source_name>/<resource>"
   ```
   The launch must end with `RUN_SUCCESS`. Inspect output: `gzcat /tmp/dlt-test/<source_name>_raw/<resource>/*.jsonl.gz | head`.
5. **Run twice when incremental is configured** — the second run should load zero/few new records.

## Known gotchas (verified in this repo)

- `processing_steps` maps run **before** the incremental cursor is read. `cursor_path` and `primary_key` must use the field names the map **emits**, or the run fails with `IncrementalCursorPathMissing`.
- Partitioned loads require the hand-written `RESTClient` style — per-run window bounds cannot flow into a declarative `rest_api_source`. Cursor values and window bounds are ISO-8601 `Z` strings by repo convention.
- Re-running the same partition writes duplicate rows: `append` appends, and `merge` (which the adam resources declare) is only honored by the `filesystem` destination with delta/iceberg table formats — on plain files it also just adds new ones ([details](docs/pipelines-and-destinations.md#write-dispositions-on-object-storage)). Dedupe downstream on `id`/`_dlt_load_id`, or clear that window's files first.
- Secrets/API tokens are read via `dlt.secrets["datasources.<name>.<key>"]` from `.dlt/secrets.toml` or `DATASOURCES__<NAME>__<KEY>` env vars — never hardcode.
- The MinIO endpoint goes in `[destination.filesystem.credentials] endpoint_url`, never inside `bucket_url`.
- The component YAML file must be named exactly `defs.yaml` or the folder silently loads as a plain module.
- A folder with `defs.yaml` is entirely component-owned: sibling `.py` files there are **not** autoloaded. Schedules, sensors, and downstream assets go in plain modules outside component folders (e.g. `defs/schedules.py`).
- `dagster` pins `protobuf<7`, which pins `grpcio<1.80` in pyproject — do not bump these.
- If a pipeline misbehaves after config changes, reset its local state: `rm -rf ~/.dlt/pipelines/<pipeline_name>` (it re-syncs from the destination).
- Test artifacts (`*.duckdb`, local `file://` buckets) must not be committed; `*.duckdb` is gitignored.

## Reference implementation

`src/datasources/defs/mdharura/` is the canonical example: a hand-written `RESTClient` resource with `page_number` pagination, an inline filter (only signal codes in `SIGNALS_OF_INTEREST`) plus a mapping function reshaping records before yield, and **daily partitions** — the resource declares a `dlt.sources.incremental("created_at")` argument and passes its bounds to the API as `dateStart`/`dateEnd`; the component binds each run's partition window onto those bounds (ISO-8601 `Z` strings). `defs.yaml` sets `partitions_def` + `backfill_policy: single_run`, so backfilling history is `dg launch --assets "<key>" --partition-range "<start>...<end>"` (one windowed run). Jobs/schedules live in `defs/schedules.py`: `define_asset_job` per source + `build_schedule_from_partitioned_job`.

`src/datasources/defs/adam/` shows the variations: one component folder with **two loader modules** (`evd_cases_loader.py`, `evd_travellers_loader.py`), each exposing its own `source`/`pipeline` pair that `defs.yaml` references by module name (`.evd_cases_loader.source`, …); a **POST** API paged via `PageNumberPaginator(base_page=0, page_body_path="page", total_path=None)` with the partition window sent in the request body (`timestamp_start`/`timestamp_end`); and a server-side `projection` in the body instead of a client-side map.

Partition test for a partitioned source (verify counts against the API):

```bash
DESTINATION__FILESYSTEM__BUCKET_URL="file:///tmp/dlt-test" \
  uv run dg launch --assets "<source>/<resource>" --partition "2026-07-06"
```

## Contribution workflow

Fork → branch `features/<source_name>` off `dev` → PR into `dev` (not `main`). Details: [docs/contributing.md](docs/contributing.md). Before opening the PR, the checklist in [docs/adding-a-source.md](docs/adding-a-source.md#checklist-before-opening-a-pr) must pass.
