"""Load travellers surveillance data from ADaM API into MinIO.

ADaM is the tool used to screen travellers at the point of entry.

The travellers resource is windowed on created_timestamp: each Dagster partition run
loads one day (timestamp_start/timestamp_end), so history backfills are ordinary Dagster
backfills over the partition range.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

client = RESTClient(
    base_url="https://api.adam.health.go.ke/api/records/composite",
    paginator=PageNumberPaginator(base_page=0, page_body_path="page", total_path=None),
)

TOOL_ID = "59635360-67c3-11ef-8f3c-c9e80a669bbc"
INITIAL_TIMESTAMP = "2026-05-01T00:00:00.000Z"
CURSOR = "created_timestamp"
PROJECTION = {
  "id": "id",
  "name": "name_of_traveler",
  "sex": "sex",
  "date_of_birth": "date_of_birth",
  "nationality": "country_of_nationality",
  "identifier": "id_number",
  "classification": "traveller_symptoms_ebola_classification_of_traveller",
  "screened": "Yes",
  "point_of_entry": "point_of_entry",
  "created_timestamp": "created_timestamp"
}

@dlt.source(name="adam")
def adam_source():
    @dlt.resource(name="travellers", primary_key="id", write_disposition="merge")
    def travellers(
        created_timestamp=dlt.sources.incremental(
            CURSOR, initial_value=INITIAL_TIMESTAMP
        ),
    ):
        body = {
            "limit": 250,
            "tool_id": TOOL_ID,
            "complete": "true",
            "format": "tabular",
            "projection": PROJECTION,
        }
        if created_timestamp.last_value:
            body["timestamp_start"] = created_timestamp.last_value

        if created_timestamp.end_value:
            body["timestamp_end"] = created_timestamp.end_value

        for page in client.paginate("", method="POST", json=body, data_selector="payload.rows"):
            yield page

    return travellers


source = adam_source()

pipeline = dlt.pipeline(
    pipeline_name="adam_evd_travellers",
    destination="filesystem",
    dataset_name="adam_travellers_raw",
    progress=dlt.progress.tqdm(colour="yellow"), 
)
