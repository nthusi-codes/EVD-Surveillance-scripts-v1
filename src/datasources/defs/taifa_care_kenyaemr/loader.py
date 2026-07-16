"""Load passively-flagged surveillance cases from Taifa Care (KenyaEMR) into MinIO.

The DMI (Disease Management Information) system on the KenyaHMIS platform
exposes cases that KenyaEMR has flagged against notifiable/priority conditions,
windowed on created_at (startDate/endDate) so each partition run loads one day.

Records are narrowed to a small demographic/location subset as they stream
through; the subject's NUPI and address are dropped, but date of birth is
retained, so the raw bucket holds indirectly-identifying data.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

BASE_URL = "https://dmistaging.kenyahmis.org/api/"
TOKEN_URL = "https://keycloak.kenyahmis.org/realms/dmi/protocol/openid-connect/token"

# earliest data to load; the partition start_date in defs.yaml must match
INITIAL_VALUE = "2025-01-01T00:00:00.000Z"


def _client() -> RESTClient:
    """Build a RESTClient at run time so `dg check defs` (import only) never
    needs credentials."""
    secrets = dlt.secrets["datasources.taifa_care_kenyaemr"]
    return RESTClient(
        base_url=BASE_URL,
        auth=OAuth2ClientCredentials(
            access_token_url=TOKEN_URL,
            client_id=secrets["client_id"],
            client_secret=secrets["client_secret"],
        ),
        paginator=PageNumberPaginator(base_page=0, total_path="data.totalPages"),
    )


def map_case(c: dict) -> dict:
    """Reshape a case to the flat subset loaded from this source."""
    subject = c.get("subject") or {}
    return {
        # id is the primary key; created_at is the incremental cursor — the
        # resource fails without both
        "id": c.get("caseUniqueId"),
        "patient_id": subject.get("patientUniqueId"),
        "sex": subject.get("sex"),
        "date_of_birth": subject.get("dateOfBirth"),
        "county": subject.get("county"),
        "sub_county": subject.get("subCounty"),
        "hospital_id": c.get("hospitalIdNumber"),
        "interview_date": c.get("interviewDate"),
        "created_at": c.get("createdAt"),
    }


@dlt.source(name="taifa_care_kenyaemr", max_table_nesting=0)
def taifa_care_kenyaemr_source():
    @dlt.resource(name="flagged_cases", primary_key="id", write_disposition="append")
    def flagged_cases(
        created_at=dlt.sources.incremental("created_at", initial_value=INITIAL_VALUE),
    ):
        client = _client()
        params = {"size": 100, "startDate": created_at.last_value[:10]}
        if created_at.end_value:
            params["endDate"] = created_at.end_value[:10]
        for page in client.paginate("case", params=params, data_selector="data.content"):
            yield [map_case(c) for c in page]

    return flagged_cases


source = taifa_care_kenyaemr_source()

pipeline = dlt.pipeline(
    pipeline_name="taifa_care_kenyaemr",
    destination="filesystem",
    dataset_name="taifa_care_kenyaemr_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
