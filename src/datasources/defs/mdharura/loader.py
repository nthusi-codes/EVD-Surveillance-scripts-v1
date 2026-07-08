"""Load event-based surveillance signals (tasks) from the m-Dharura API into MinIO.

m-Dharura is Kenya's event-based surveillance (EBS) system. The Data Export
endpoints are documented at:
https://api.m-dharura.health.go.ke/swaggerui/#/Data%20Export

The signals resource is windowed on created_at: each Dagster partition run
loads one day (dateStart/dateEnd), so history backfills are ordinary Dagster
backfills over the partition range.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

client = RESTClient(
    base_url="https://api.m-dharura.health.go.ke/v1/",
    paginator=PageNumberPaginator(base_page=1, total_path="pages"),
)

# only these EBS signal codes are loaded; the API has no signal query param,
# so records are filtered as they stream through the generator
SIGNALS_OF_INTEREST = {"7", "8"}


def map_task(task: dict) -> dict:
    """Reshape each task record before it is written to the destination."""
    unit = task.get("unit") or {}
    subcounty = unit.get("parent") or {}
    county = subcounty.get("parent") or {}

    return {
        "id": task.get("_id"),
        "signal": task.get("signal"),
        "community_unit": unit.get("name"),
        "subcounty": subcounty.get("name"),
        "county": county.get("name"),
        "created_at": task.get("createdAt"),
    }


@dlt.source(name="mdharura")
def mdharura_source():
    @dlt.resource(name="signals", primary_key="id", write_disposition="append")
    def signals(
        created_at=dlt.sources.incremental(
            "created_at", initial_value="2026-06-01T00:00:00.000Z"
        ),
    ):
        params = {
            "limit": 100,
            "state": "live",
            "dateStart": created_at.last_value,
        }
        if created_at.end_value:
            params["dateEnd"] = created_at.end_value
        for page in client.paginate("export/tasks", params=params):
            yield [
                map_task(task)
                for task in page
                if str(task.get("signal")) in SIGNALS_OF_INTEREST
            ]

    return signals


source = mdharura_source()

pipeline = dlt.pipeline(
    pipeline_name="mdharura",
    destination="filesystem",
    dataset_name="mdharura_raw",
)
