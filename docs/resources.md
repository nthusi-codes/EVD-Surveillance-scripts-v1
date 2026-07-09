# Defining resources

A **resource** is one stream of records from a source — one API endpoint, one file feed, one scrape target. Each resource becomes one Dagster asset and one table (folder of files) in the destination. This page covers the three ways to define them, from most to least declarative, plus transformations and incremental loading.

## Option 1: declarative REST (`rest_api_source`)

Best default for simple, non-partitioned REST APIs. You describe the API; dlt handles requests, pagination, retries, and streaming:

```python
from dlt.sources.rest_api import rest_api_source

source = rest_api_source(
    {
        "client": {
            "base_url": "https://api.m-dharura.health.go.ke/v1/",
            # for authenticated APIs — token comes from .dlt/secrets.toml:
            # "auth": {"type": "bearer",
            #          "token": dlt.secrets["datasources.mdharura.api_token"]},
            "paginator": {
                "type": "page_number",   # ?page=1,2,3... until `pages` is reached
                "base_page": 1,
                "total_path": "pages",   # response field holding the page count
            },
        },
        "resources": [
            {
                "name": "tasks",
                "primary_key": "_id",
                "write_disposition": "append",
                "endpoint": {
                    "path": "export/tasks",
                    "params": {"limit": 500, "state": "live"},
                    "data_selector": "data",   # records live under {"data": [...]}
                },
            },
        ],
    },
    name="mdharura",
)
```

Common paginator types: `page_number` (as above), `json_link` (next-page URL in the response body), `header_link` (RFC 5988 `Link` header), `offset`, `cursor`. See the [rest_api docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/) for the full matrix.

## Option 2: hand-written resource with `RESTClient`

Drop to this when you need **page-level control** (reading the response envelope, transforming whole pages, cross-page state, early stopping) or **partitioned/windowed loading** — per-run date bounds only flow into a hand-written resource. You keep dlt's paginators and retry handling.

[`mdharura/loader.py`](../src/datasources/defs/mdharura/loader.py) is the working example. Its resource declares a `dlt.sources.incremental` argument and passes the bounds to the API — which is what lets our component bind each Dagster partition's time window onto the request ([how backfills work](dagster.md#partitions-and-backfills)):

```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

client = RESTClient(
    base_url="https://api.m-dharura.health.go.ke/v1/",
    paginator=PageNumberPaginator(base_page=1, total_path="pages"),
)

SIGNALS_OF_INTEREST = {"7", "8", "H4"}

@dlt.source(name="mdharura")
def mdharura_source():

    @dlt.resource(name="signals", primary_key="id", write_disposition="append")
    def signals(
        created_at=dlt.sources.incremental("created_at", initial_value="2026-06-01T00:00:00.000Z"),
    ):
        params = {"limit": 100, "state": "live", "dateStart": created_at.last_value}
        if created_at.end_value:
            params["dateEnd"] = created_at.end_value
        for page in client.paginate("export/tasks", params=params):
            yield [
                map_task(task)
                for task in page
                if str(task.get("signal")) in SIGNALS_OF_INTEREST
            ]

    return signals
```

`client.paginate()` yields one page at a time; `page.response` is the raw `requests.Response` when you need the envelope. Yield whatever you want records to be — a list per page, one record at a time, or a summary row.

The adam loaders ([`evd_cases_loader.py`](../src/datasources/defs/adam/evd_cases_loader.py), [`evd_travellers_loader.py`](../src/datasources/defs/adam/evd_travellers_loader.py)) are the same pattern against a **POST** API: `PageNumberPaginator(base_page=0, page_body_path="page", total_path=None)` writes the page number into the JSON request body and stops on the first empty page, the incremental bounds go into the body as `timestamp_start`/`timestamp_end`, and a server-side `projection` in the body reshapes records at the API — so no client-side map is needed.

## Option 3: plain generator

For non-HTTP sources (files, databases, scrapes), any generator works:

```python
@dlt.resource(name="lab_results", primary_key="sample_id", write_disposition="merge")
def lab_results():
    for path in bucket.list("incoming/"):
        yield from parse_result_file(path)
```

## Filtering and transforming records before the destination

Transforms and filters run **record-by-record, streaming, during extraction** — nothing is written until they've run. Filtered-out records never reach the bucket.

**In hand-written resources**, filter and map inline in the generator — you control exactly what gets yielded. This is how mdharura keeps only the signal codes it cares about (the API has no signal query param, so it must happen client-side):

```python
SIGNALS_OF_INTEREST = {"7", "8", "H4"}

for page in client.paginate("export/tasks", params=params):
    yield [
        map_task(task)                                   # transform
        for task in page
        if str(task.get("signal")) in SIGNALS_OF_INTEREST  # filter
    ]
```

Prefer a server-side filter (query param) whenever the API offers one — it avoids fetching records just to drop them. Client-side filtering still pages through everything; it only keeps the bucket clean.

**In declarative configs**, use `processing_steps` per resource:

```python
"resources": [
    {
        "name": "tasks",
        "endpoint": {"path": "export/tasks", "data_selector": "data"},
        "processing_steps": [
            {"filter": lambda r: r["status"] == "completed"},
            {"map": redact_phone_numbers},   # def redact_phone_numbers(record) -> record
        ],
    },
],
```

On any resource object, use `add_map` / `add_filter` / `add_yield_map` (one-to-many):

```python
for resource in source.resources.values():
    resource.add_map(redact_pii)
```

For enrichment that needs another API call per record, use a transformer — it becomes its own asset/table:

```python
@dlt.transformer(data_from=tasks, primary_key="_id")
def task_units(task):
    yield from fetch_unit_details(task["units"])
```

**Boundary:** these hooks are for row/page-level reshaping (rename, filter, coerce, redact). Joins, aggregations, and cross-dataset dedup belong in a downstream Dagster asset that reads from the bucket — keep the raw layer replayable.

**Gotcha — maps run before the incremental cursor is read.** If a map renames or drops the cursor field, the run fails with `IncrementalCursorPathMissing`. Point `cursor_path` at the field name **your map emits**, not the API's raw name. In the mdharura source, `map_task` emits `created_at` (from the API's `createdAt`), so the incremental config uses `cursor_path: "created_at"`.

## Incremental loading

Full reloads (`write_disposition="replace"`) are fine for small reference data but wasteful for large or growing endpoints — m-Dharura has 225k+ tasks. Two incremental patterns:

**Partition-windowed (preferred — what mdharura and adam use).** The resource declares a `dlt.sources.incremental` argument (see Option 2 above) and the asset is time-partitioned in `defs.yaml`; each run loads exactly its partition's window and backfills are Dagster-native. Details: [dagster.md — Partitions and backfills](dagster.md#partitions-and-backfills).

**Cursor-state (declarative sources).** In `rest_api_source` configs, an `incremental` block on the endpoint makes dlt remember the newest cursor seen and pass it as a query param on the next run:

```python
"endpoint": {
    "path": "records",
    "data_selector": "data",
    "incremental": {
        "start_param": "dateStart",       # query param the API filters on
        "cursor_path": "created_at",      # field dlt tracks
        "initial_value": "2021-09-01T00:00:00.000Z",
    },
},
"write_disposition": "append",
```

The cursor lives in pipeline state between runs; to re-backfill, change `initial_value` and reset the pipeline state (see [pipelines-and-destinations.md](pipelines-and-destinations.md#pipeline-state-and-troubleshooting)). Full docs: [incremental loading](https://dlthub.com/docs/general-usage/incremental-loading).

Caveat (both patterns): a `created_at` cursor only picks up **new** records. m-Dharura tasks are updated after creation (verification/response forms get filled in), so records loaded early may go stale until affected partitions are re-run or an `updatedAt`-based strategy is added. And don't count on `write_disposition="merge"` to reconcile re-loaded records — it degrades to append on our filesystem destination ([details](pipelines-and-destinations.md#write-dispositions-on-object-storage)).

## Schema control

- `max_table_nesting=0` on a source stops dlt from exploding nested objects into child tables (useful for keeping raw API records intact; mdharura instead maps records flat before yielding, which achieves the same thing explicitly).
- `columns={...}` on a resource pins types when inference guesses wrong.
- Without a nesting limit, nested lists become child tables named `<resource>__<field>` — you'd see them as extra folders in the bucket; that's expected.
