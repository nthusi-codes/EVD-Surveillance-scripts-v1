"""Load EVD quarantine records from KRCS Data Capture Forms into MinIO.

One row per Person of Concern under the 21-day post-arrival EVD quarantine:
demographics, origin, quarantine site, workflow status and the expected
completion date. Records are pseudonymised at the source.

The resource is windowed on `modified`, not creation time, so a record
reappears when its status changes or a new daily review is added. Writes are
`append`, so deduplicate downstream on `quarantineIdentifier`, keeping the
latest `modified`.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Frappe wraps whitelisted responses in a `message` envelope.
ENDPOINT = "data_capture_forms.evd_screening.quarantine_lake.records"
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
def krcs_evd_quarantine_records_source():
    @dlt.resource(name="quarantine_records", primary_key="quarantineIdentifier", write_disposition="append")
    def quarantine_records(
        modified=dlt.sources.incremental("modified", initial_value="2026-06-01T00:00:00.000Z"),
    ):
        params = {"limit": PAGE_SIZE, "dateStart": modified.last_value}
        if modified.end_value:
            params["dateEnd"] = modified.end_value
        yield from _client().paginate(ENDPOINT, params=params, data_selector="message.data")

    return quarantine_records


source = krcs_evd_quarantine_records_source()

pipeline = dlt.pipeline(
    pipeline_name="krcs_evd_quarantine_records",
    destination="filesystem",
    dataset_name="krcs_evd_quarantine_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
