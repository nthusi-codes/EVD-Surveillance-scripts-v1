"""Project component types, registered via [tool.dg] registry_modules in pyproject.toml."""

import ast
import textwrap
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import dlt
from dagster.components.core.context import ComponentLoadContext
from dagster_dlt import DltLoadCollectionComponent as _DltLoadCollectionComponent
from dagster_dlt.constants import META_KEY_SOURCE
from dagster.components.scaffold.scaffold import ScaffoldRequest, scaffold_with


def _iso_z(dt: datetime) -> str:
    """Format a datetime the way the source APIs and cursors do: ISO-8601 UTC with a Z."""
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _cursor_path(resource) -> str | None:
    """The dlt incremental cursor path of a resource, from an applied hint or
    the resource function's signature default (`x=dlt.sources.incremental(...)`)."""
    import inspect

    from dlt.extract.incremental import IncrementalResourceWrapper

    wrapper = resource.incremental
    if wrapper is None:
        return None
    if wrapper.incremental is not None:
        return wrapper.incremental.cursor_path
    param = IncrementalResourceWrapper.get_incremental_arg(
        inspect.signature(resource._pipe.gen)
    )
    if param is not None and param.default is not inspect.Parameter.empty:
        return param.default.cursor_path
    return None

LOADER_TEMPLATE = '''\
"""{name}: load data from <API> into MinIO.

See docs/developer-walkthrough.md for the step-by-step guide.
"""

import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source(
    {{
        "client": {{
            "base_url": "https://api.example.org/v1/",
            "paginator": {{
                "type": "page_number",
                "base_page": 1,
                "total_path": "pages",
            }},
        }},
        "resources": [
            {{
                "name": "records",
                "primary_key": "id",
                "write_disposition": "append",
                "endpoint": {{
                    "path": "records",
                    "params": {{"limit": 50}},
                    "data_selector": "data",
                }},
            }},
        ],
    }},
    name="{name}",
    max_table_nesting=0,
)

pipeline = dlt.pipeline(
    pipeline_name="{name}",
    destination="filesystem",
    dataset_name="{name}_raw",
)
'''


class DltLoaderScaffolder(dg.Scaffolder):
    """Scaffolds a data-source folder following this repo's conventions:

    - the Python module is named loader.py (upstream default is loads.py)
    - it exposes module-level `source` and `pipeline` objects
    - defs.yaml is pre-filled with the asset-key/group translation derived
      from the folder name
    """

    def scaffold(self, request: ScaffoldRequest) -> None:
        target = Path(request.target_path)
        name = target.name
        target.mkdir(parents=True, exist_ok=True)
        (target / "loader.py").write_text(
            textwrap.dedent(LOADER_TEMPLATE).format(name=name), encoding="utf-8"
        )
        dg.scaffold_component(
            request=request,
            yaml_attributes={
                "loads": [
                    {
                        "source": ".loader.source",
                        "pipeline": ".loader.pipeline",
                        "translation": {
                            "key": name + "/{{ resource.name }}",
                            "group_name": name,
                        },
                    }
                ]
            },
        )


@scaffold_with(DltLoaderScaffolder)
class DltLoadSourceCollection(_DltLoadCollectionComponent):
    """dlt load collection whose scaffold follows this repo's conventions (loader.py).

    Like the dbt integration surfaces each model's SQL, every asset's
    description ends with the source of the loader module that defines its
    dlt source, rendered as a Python code block in the UI. The module is found
    by matching each asset's dlt source against the `source` objects of the
    .py files in the component folder, so a folder may hold one loader.py or
    several (e.g. adam's evd_cases_loader.py / evd_travellers_loader.py). The
    leading summary line is the module docstring's first line.

    Loads with a partitions_def get windowed execution: the run's partition
    time window is bound onto every resource that declares a dlt incremental,
    as ISO-8601 Z strings (initial_value/end_value). With a single_run
    backfill policy, a whole backfill range becomes one windowed run.
    """

    def execute(self, context, dlt_pipeline_resource):
        # has_partition_key is True only for single-partition runs; ranged
        # runs (single_run backfills) set a partition key range instead
        if not (context.has_partition_key or context.has_partition_key_range):
            yield from super().execute(context, dlt_pipeline_resource)
            return

        window = context.partition_time_window
        metadata = next(iter(context.assets_def.metadata_by_key.values()))
        source = metadata[META_KEY_SOURCE]
        for resource in source.resources.values():
            cursor_path = _cursor_path(resource)
            if cursor_path is None:
                continue
            resource.apply_hints(
                incremental=dlt.sources.incremental(
                    cursor_path,
                    initial_value=_iso_z(window.start),
                    end_value=_iso_z(window.end),
                )
            )
        yield from dlt_pipeline_resource.run(context=context, dlt_source=source)

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        defs = super().build_defs(context)

        # the yaml's `.some_module.source` paths are consumed during resolution,
        # so recover each load's defining module by importing the folder's .py
        # files and matching their `source` objects by identity
        docs_by_source_id: dict[int, tuple[str, str]] = {}
        for loader_path in sorted(Path(context.path).glob("*.py")):
            module = context.load_defs_relative_python_module(loader_path)
            module_source = getattr(module, "source", None)
            if module_source is None:
                continue
            code = loader_path.read_text(encoding="utf-8")
            docstring = ast.get_docstring(ast.parse(code)) or ""
            summary = (
                docstring.splitlines()[0]
                if docstring
                else f"Defined in {loader_path.name}."
            )
            docs_by_source_id[id(module_source)] = (summary, code)

        def _attach(spec: dg.AssetSpec) -> dg.AssetSpec:
            doc = docs_by_source_id.get(id(spec.metadata.get(META_KEY_SOURCE)))
            if doc is None:
                return spec
            summary, code = doc
            # summary first so list views show readable text; the full source
            # renders as a code block on the asset page
            return spec.replace_attributes(
                description=f"{spec.description or summary}\n\n```python\n{code}\n```"
            )

        return defs.map_asset_specs(func=_attach)


# keep the imported base class out of the component registry
del _DltLoadCollectionComponent
