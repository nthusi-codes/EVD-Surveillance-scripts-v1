# How Dagster ties it together

## Autoloading

[`definitions.py`](../src/datasources/definitions.py) calls `load_from_defs_folder`, which walks `src/datasources/defs/` recursively and loads everything it finds. Two kinds of content are picked up:

1. **Component folders** — any folder containing a file named exactly `defs.yaml` (the name is how Dagster recognizes a component; nothing else works). Our dlt sources use `type: datasources.components.DltLoadSourceCollection` (our project-local component — see [`datasources/components`](../src/datasources/components/__init__.py)).
2. **Plain Python modules** — any `.py` file defining Dagster objects (`@dg.asset`, `ScheduleDefinition`, sensors, asset checks) in folders **without** a `defs.yaml`.

Important: these are mutually exclusive per folder. Once a folder contains `defs.yaml`, the whole folder belongs to that component — sibling `.py` files there (other than the ones `defs.yaml` references, like `loader.py`) are **not** autoloaded. Schedules, downstream assets, and sensors must live in plain modules outside component folders.

Consequence: **never import or register anything centrally.** Drop a folder in `defs/`, and it's live.

## Asset keys, groups, and dependencies

Each dlt resource becomes an asset. The `translation` block in `defs.yaml` controls naming:

```yaml
loads:
  - source: .loader.source
    pipeline: .loader.pipeline
    translation:
      key: "mdharura/{{ resource.name }}"
      group_name: mdharura
```

Each asset produced by our component also self-documents, dbt-style. Its **description** is a plain-text summary line followed by the full `loader.py` source rendered as a Python code block — the summary shows in asset lists, the code on the asset page. The summary is `translation.description` from `defs.yaml` when set, otherwise the first line of the `loader.py` module docstring — so write a real docstring either way.

Convention: key prefix and group = the source folder name. Dagster also creates an upstream *external* asset per resource (e.g. `mdharura_signals`) representing the raw API — it has no materialization function; it's lineage metadata.

To build a downstream asset (e.g. a cleaned table computed from the raw bucket data), reference the dlt asset's key as a dependency from any Python file under `defs/`:

```python
import dagster as dg

@dg.asset(deps=[dg.AssetKey(["mdharura", "signals"])], group_name="mdharura")
def weekly_signal_summary():
    ...  # read s3://…/mdharura_raw/signals/, write derived output
```

## Jobs and schedules

Jobs and schedules live in [`defs/schedules.py`](../src/datasources/defs/schedules.py) — a plain module at the `defs/` root, **not** inside a source folder (component folders don't autoload sibling `.py` files; see [Autoloading](#autoloading)). Each source gets an asset job (selected by group) and, for partitioned sources, a schedule derived from the job's partitions:

```python
import dagster as dg

mdharura_sync_job = dg.define_asset_job(
    name="mdharura_sync_job",
    selection=dg.AssetSelection.groups("mdharura"),
    description="Loads m-Dharura signals for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 06:00 UTC for the previous day
sync_mdharura_signals_daily = dg.build_schedule_from_partitioned_job(
    mdharura_sync_job,
    hour_of_day=6,
    name="sync_mdharura_signals_daily",
    description="Syncs the previous day's signals from m-Dharura every day at 06:00 UTC",
)
```

Naming: `<source>_sync_job` and `sync_<source>_<resource>_<cadence>`. Schedules only fire when the daemon is running (`dg dev` runs one locally; production deployments run `dagster-daemon`).

## Partitions and backfills

The mdharura load is **daily-partitioned** (see its [`defs.yaml`](../src/datasources/defs/mdharura/defs.yaml)): each partition run loads exactly one day's records, and loading history is an ordinary Dagster backfill. Three pieces make this work:

1. `partitions_def` + `backfill_policy: single_run` on the load in `defs.yaml`. With `single_run`, a backfill over any partition range collapses into **one windowed run** instead of one run per day.
2. Our component ([`datasources/components`](../src/datasources/components/__init__.py)) binds the run's partition time window onto every resource that declares a `dlt.sources.incremental` — as ISO-8601 `Z` strings via `initial_value`/`end_value`.
3. The loader's resource reads those bounds and passes them to the API (`dateStart`/`dateEnd` in [`mdharura/loader.py`](../src/datasources/defs/mdharura/loader.py)). This requires the hand-written `RESTClient` style ([resources.md, option 2](resources.md#option-2-hand-written-resource-with-restclient)) — the declarative `rest_api_source` can't receive per-run bounds.

Running backfills:

```bash
# one day
dg launch --assets "mdharura/signals" --partition "2026-07-06"
# a range — becomes a single windowed run (single_run policy)
dg launch --assets "mdharura/signals" --partition-range "2026-06-01...2026-07-01"
```

or in the UI: asset page → **Materialize** → select partitions / **Backfill**.

Caveats: partition runs use `write_disposition="append"`, so **re-running the same partition appends duplicates** — dedupe downstream on `id` (or `_dlt_load_id`) or delete that day's files from the bucket first. Timestamps use the ISO-8601 `Z` convention throughout (cursor values, API params); a source with a different cursor format needs its own conversion in the loader.

## Useful commands

| Command | What it does |
| --- | --- |
| `dg dev` | Web UI + daemon at http://localhost:3000 |
| `dg check defs` | Validate that all definitions load — run before every PR |
| `dg list defs` | List all assets/schedules/etc. in the terminal |
| `dg launch --assets "<key>"` | Materialize assets headless |
| `dg list components` | Available component types for scaffolding |
| `dg scaffold defs <component> <name>` | Scaffold a new source folder |

Run them as `uv run dg ...` if the venv isn't activated.

## Deployment

`docker compose up -d --build` runs the production layout: one image, two containers (webserver + daemon), SQLite state in a shared volume, runs executing inside the daemon container. Full guide — architecture, configuration, operations, troubleshooting, scaling path: [deployment.md](deployment.md).

## Gotchas

- **Asset not showing up?** Run `dg check defs` — import errors in any file under `defs/` are reported there. A folder with a misnamed `defs.yml`/`component.yaml` loads as a plain module and its YAML is silently ignored.
- **grpcio/protobuf pin:** `dagster` requires `protobuf<7`, so `grpcio` is pinned `<1.80` in [`pyproject.toml`](../pyproject.toml) (newer grpcio ships protobuf-7 generated code). Don't bump it until Dagster lifts the cap.
- **Dependencies:** add runtime deps with `uv add <pkg>`, dev-only tooling with `uv add --dev <pkg>`.
