"""Load event-based surveillance signals (tasks) from the eCHIS API into MinIO.

eCHIS is Kenya's community health systems
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

client = RESTClient(
    base_url="https://echis.health.go.ke/postgres-api-access",
    paginator=SinglePagePaginator()
)

# only these EBS signal codes are loaded; the API has no signal query param,
# so records are filtered as they stream through the generator
#SIGNALS_OF_INTEREST = {"7", "8", "H4"}

def map_report(report: dict) -> dict:
    """Reshape each task record before it is written to the destination."""
    return report


@dlt.source(name="echis")
def echis_source():
    @dlt.resource(name="signals", primary_key="case_id", write_disposition="append")
    def signals():
        params = {}
        for page in client.paginate("echis_signals_evd", params=params):
            yield [
                map_report(report)
                for report in page
                if bool(report.get("case_id"))
            ]

    return signals


source = echis_source()

pipeline = dlt.pipeline(
    pipeline_name="echis",
    destination="filesystem",
    dataset_name="echis_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
