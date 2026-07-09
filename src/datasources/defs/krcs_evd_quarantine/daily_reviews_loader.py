"""Load EVD quarantine daily clinical reviews from KRCS Data Capture Forms into MinIO.

One row per daily review taken during a Person of Concern's 21-day quarantine:
the quarantine day number, body temperature (LOINC 8310-5), blood pressure,
condition and complaints, linked back to its quarantine record. Pseudonymised at
the source.

The resource is windowed on `modified`. Writes are `append`, so deduplicate
downstream on `reviewIdentifier`, keeping the latest `modified`.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Frappe wraps whitelisted responses in a `message` envelope.
ENDPOINT = "data_capture_forms.evd_screening.quarantine_lake.daily_reviews"
PAGE_SIZE = 200


def _client() -> RESTClient:
    # Built lazily so a missing credential fails the run, not `dg check defs`.
    base_url = dlt.secrets["datasources.krcs_evd_quarantine.base_url"]
    key = dlt.secrets["datasources.krcs_evd_quarantine.api_key"]
    secret = dlt.secrets["datasources.krcs_evd_quarantine.api_secret"]

    missing = [n for n, v in (("base_url", base_url), ("api_key", key), ("api_secret", secret)) if not v]
    if missing:
        raise ValueError(f"datasources.krcs_evd_quarantine: missing or empty {', '.join(missing)}")

    return RESTClient(
        base_url=base_url,
        headers={"Authorization": f"token {key}:{secret}"},
        paginator=PageNumberPaginator(base_page=1, total_path="message.pages"),
    )


@dlt.source(name="krcs_evd_quarantine", max_table_nesting=0)
def krcs_evd_quarantine_daily_reviews_source():
    @dlt.resource(name="daily_reviews", primary_key="reviewIdentifier", write_disposition="append")
    def daily_reviews(
        modified=dlt.sources.incremental("modified", initial_value="2026-06-01T00:00:00.000Z"),
    ):
        params = {"limit": PAGE_SIZE, "dateStart": modified.last_value}
        if modified.end_value:
            params["dateEnd"] = modified.end_value
        yield from _client().paginate(ENDPOINT, params=params, data_selector="message.data")

    return daily_reviews


source = krcs_evd_quarantine_daily_reviews_source()

pipeline = dlt.pipeline(
    pipeline_name="krcs_evd_quarantine_daily_reviews",
    destination="filesystem",
    dataset_name="krcs_evd_quarantine_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
