"""Load point-of-entry health screenings from KRCS Data Capture Forms into MinIO.

Screenings are recorded at airports, seaports and land crossings and moved
through a surveillance workflow. They are pseudonymised at the source.

The resource is windowed on `modified`, not creation time, so a screening
reappears when its workflow state changes. Writes are `append`, so deduplicate
downstream on `screening_identifier`, keeping the latest `modified`.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Frappe wraps whitelisted responses in a `message` envelope.
ENDPOINT = "data_capture_forms.evd_screening.lake.screenings"
PAGE_SIZE = 200


def _client() -> RESTClient:
    # Built lazily so a missing credential fails the run, not `dg check defs`.
    base_url = dlt.secrets["datasources.krcs_evd_screening.base_url"]
    key = dlt.secrets["datasources.krcs_evd_screening.api_key"]
    secret = dlt.secrets["datasources.krcs_evd_screening.api_secret"]

    missing = [n for n, v in (("base_url", base_url), ("api_key", key), ("api_secret", secret)) if not v]
    if missing:
        raise ValueError(f"datasources.krcs_evd_screening: missing or empty {', '.join(missing)}")

    return RESTClient(
        base_url=base_url,
        headers={"Authorization": f"token {key}:{secret}"},
        paginator=PageNumberPaginator(base_page=1, total_path="message.pages"),
    )


@dlt.source(name="krcs_evd_screening", max_table_nesting=0)
def krcs_evd_screening_source():
    @dlt.resource(name="screenings", primary_key="screeningIdentifier", write_disposition="append")
    def screenings(
        modified=dlt.sources.incremental("modified", initial_value="2026-06-01T00:00:00.000Z"),
    ):
        params = {"limit": PAGE_SIZE, "dateStart": modified.last_value}
        if modified.end_value:
            params["dateEnd"] = modified.end_value
        yield from _client().paginate(ENDPOINT, params=params, data_selector="message.data")

    return screenings


source = krcs_evd_screening_source()

pipeline = dlt.pipeline(
    pipeline_name="krcs_evd_screening",
    destination="filesystem",
    dataset_name="krcs_evd_screening_raw",
)
