# evd_surveillance_scripts

A [Dagster](https://docs.dagster.io/) + [dlt](https://dlthub.com/docs) data platform for EVD surveillance data sources. Each data source lives in its own self-contained folder under `src/datasources/defs/` and is **discovered automatically** — adding a new source never requires touching shared code.

```
src/datasources/
├── definitions.py              # autoloads everything under defs/ — never edit
└── defs/
    ├── adam/                   # one folder per data source
    │   ├── defs.yaml           # wires the dlt loads into Dagster
    │   ├── evd_cases_loader.py       # a folder can hold several loaders,
    │   └── evd_travellers_loader.py  # each with its own source + pipeline
    ├── lims/
    └── mdharura/
        ├── defs.yaml
        └── loader.py           # the dlt source (API calls) + pipeline (destination)
```

## Quickstart

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/) and Python ≥ 3.10.

```bash
git clone <this-repo> && cd evd-surveillance-scripts
uv sync                        # creates .venv and installs everything
source .venv/bin/activate      # optional — `uv run <cmd>` works without it
```

Configure the MinIO destination (see [docs/pipelines-and-destinations.md](docs/pipelines-and-destinations.md)):

```bash
# .dlt/secrets.toml  (gitignored — never commit)
[destination.filesystem.credentials]
aws_access_key_id = "..."                 # your MinIO access key
aws_secret_access_key = "..."
endpoint_url = "http://localhost:9000"    # your MinIO server
```

Run it:

```bash
dg dev                                       # Dagster UI at http://localhost:3000
dg list defs                                 # or: list all assets in the terminal
dg launch --assets "mdharura/signals"          # or: run one asset headless
```

## Adding your own data source

```bash
dg scaffold defs datasources.components.DltLoadSourceCollection my_source
```

This creates `defs/my_source/` with a `loader.py` and a `defs.yaml` pre-filled with the repo conventions. Edit `loader.py` to fetch from your API, then verify:

```bash
dg check defs                                # validates everything loads
dg launch --assets "my_source/<resource>"    # test-run it
```

Full walkthrough: [docs/adding-a-source.md](docs/adding-a-source.md). Use [`defs/mdharura/`](src/datasources/defs/mdharura/) as a working reference — it incrementally pulls EBS signals from the [m-Dharura API](https://api.m-dharura.health.go.ke/swaggerui/) and loads to MinIO (S3-compatible object storage). [`defs/adam/`](src/datasources/defs/adam/) shows a folder with two loaders (case investigations and traveller screenings) against a POST API.

## Documentation

| Doc | Covers |
| --- | --- |
| [docs/developer-walkthrough.md](docs/developer-walkthrough.md) | Full tutorial: dev setup, MinIO, running Dagster, building a resource from scratch |
| [docs/adding-a-source.md](docs/adding-a-source.md) | Quick reference for adding a new data source |
| [docs/contributing.md](docs/contributing.md) | Fork → feature branch → PR into `dev` workflow |
| [docs/resources.md](docs/resources.md) | Defining dlt resources: REST APIs, pagination, transformations, incremental loading |
| [docs/pipelines-and-destinations.md](docs/pipelines-and-destinations.md) | Pipelines, the MinIO destination, configuration and secrets |
| [docs/dagster.md](docs/dagster.md) | How Dagster discovers sources, asset keys, schedules, useful `dg` commands |
| [docs/deployment.md](docs/deployment.md) | Docker Compose deployment: architecture, operations, troubleshooting |

## Learn more

- [dlt documentation](https://dlthub.com/docs) — the extract/load framework
- [Dagster documentation](https://docs.dagster.io/) — orchestration
- [dagster-dlt integration](https://docs.dagster.io/integrations/libraries/dlt) — how the two connect
