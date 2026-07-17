"""Load passively-flagged surveillance cases from Taifa Care (KenyaEMR) into MinIO.

The DMI system on the KenyaHMIS platform
exposes cases that KenyaEMR has flagged against notifiable/priority conditions;
only the EVD-relevant flags are requested (see FLAGS). Cases are windowed on
load_date (startDate/endDate) so each partition run loads one day.

Records are narrowed to a small demographic/location subset as they stream
through; the subject's NUPI and address are dropped, but date of birth is
retained, so the raw bucket holds indirectly-identifying data.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# earliest data to load; the partition start_date in defs.yaml must match
INITIAL_VALUE = "2025-01-01T00:00:00.000Z"

# only cases flagged against these conditions are requested
FLAGS = "EBOLA,VIRAL HAEMORRHAGIC FEVER"


def _client() -> RESTClient:
    """Build a RESTClient at run time so `dg check defs` (import only) never
    needs config or credentials."""
    base_url = dlt.secrets["datasources.taifa_care_kenyaemr.base_url"]
    token_url = dlt.secrets["datasources.taifa_care_kenyaemr.token_url"]
    client_id = dlt.secrets["datasources.taifa_care_kenyaemr.client_id"]
    client_secret = dlt.secrets["datasources.taifa_care_kenyaemr.client_secret"]

    missing = [
        n
        for n, v in (
            ("base_url", base_url),
            ("token_url", token_url),
            ("client_id", client_id),
            ("client_secret", client_secret),
        )
        if not v
    ]
    if missing:
        raise ValueError(
            f"datasources.taifa_care_kenyaemr: missing or empty {', '.join(missing)}"
        )

    return RESTClient(
        base_url=base_url,
        auth=OAuth2ClientCredentials(
            access_token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
        ),
        paginator=PageNumberPaginator(base_page=0, total_path="data.totalPages"),
    )


def map_case(c: dict) -> dict:
    """Reshape a case to the flat subset loaded from this source."""
    subject = c.get("subject") or {}
    return {
        # id is the primary key; load_date is the incremental cursor. The
        # resource fails without both
        "id": c.get("caseUniqueId"),
        "patient_id": subject.get("patientUniqueId"),
        "sex": subject.get("sex"),
        "date_of_birth": subject.get("dateOfBirth"),
        "county": subject.get("county"),
        "sub_county": subject.get("subCounty"),
        "hospital_id": c.get("hospitalIdNumber"),
        "interview_date": c.get("interviewDate"),
        "load_date": c.get("loadDate"),
    }


@dlt.source(name="taifa_care_kenyaemr", max_table_nesting=0)
def taifa_care_kenyaemr_source():
    @dlt.resource(name="flagged_cases", primary_key="id", write_disposition="append")
    def flagged_cases(
        load_date=dlt.sources.incremental("load_date", initial_value=INITIAL_VALUE),
    ):
        client = _client()
        params = {
            "size": 100,
            "flags": FLAGS,
            "startDate": load_date.last_value[:10],
        }
        if load_date.end_value:
            params["endDate"] = load_date.end_value[:10]
        for page in client.paginate("case", params=params, data_selector="data.data"):
            yield [map_case(c) for c in page]

    return flagged_cases


source = taifa_care_kenyaemr_source()

pipeline = dlt.pipeline(
    pipeline_name="taifa_care_kenyaemr",
    destination="filesystem",
    dataset_name="taifa_care_kenyaemr_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
