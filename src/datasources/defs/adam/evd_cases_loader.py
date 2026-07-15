"""Load case investigation data from ADaM API into MinIO.

ADaM is the tool used for case investigation, contact listing and tracing.

The cases resource is windowed on created_timestamp: each Dagster partition run
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

TOOL_ID = "a78b43f0-e4f0-11ee-a969-7765f1f98ba9"
INITIAL_TIMESTAMP = "2026-05-01T00:00:00.000Z"
CURSOR = "created_timestamp"
PROJECTION = {
  "id": "id",
  "name": [
    "case_demographics_family",
    " ",
    "case_demographics_given"
  ],
  "vhf_disease": "disease",
  "sex": "case_demographics_sex",
  "date_of_birth": "case_demographics_date_of_birth",
  "nationality": "case_demographics_country_of_nationality",
  "identifier": "national_id",
  "type": "type_of_record",
  "initial_classification": "initial_classification",
  "outcome": "clinical_care_outcome_of_case",
  "date_of_death": "clinical_care_date_of_death",
  "samples_collected": "laboratory_sample_collected",
  "specimen_id": "laboratory_specimen_id",
  "final_laboratory_results": "laboratory_final_laboratory_result",
  "final_classification": "laboratory_final_classification",
  "reporting_county": "reporting_county",
  "reporting_subcounty": "reporting_subcounty",
  "health_facility": "case_demographics_health_facility",
  "date_of_investigation": "date_of_investigation",
  "created_timestamp": "created_timestamp",
  "latitude": "latitude",
  "longitude": "longitude",
  "checked_by": "surveillance_tool_official_checked_by_email_address"
}

@dlt.source(name="adam")
def adam_source():
    @dlt.resource(name="cases", primary_key="id", write_disposition="merge")
    def cases(
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

    return cases


source = adam_source()

pipeline = dlt.pipeline(
    pipeline_name="adam_evd_cases",
    destination="filesystem",
    dataset_name="adam_cases_raw",
    progress=dlt.progress.tqdm(colour="yellow"), 
)
