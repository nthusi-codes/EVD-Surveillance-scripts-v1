"""Load CBS surveillance reports and EVD border screenings into MinIO.

CBS is the Kenya Red Cross Community-Based Surveillance platform. Its
Integration API (https://cbs.redcross.or.ke/api/integration/v1/, authenticated
with an x-api-key header) exposes two flat JSON list endpoints:

- reports    — surveillance reports    (scope: reports:read)
- screenings — EVD border screenings   (scope: screenings:read)

Both are windowed on the record's own date: each Dagster partition run loads
one day (fromDate/toDate), so history backfills are ordinary Dagster backfills
over the partition range. The API treats toDate as an exclusive bound at
midnight, which is exactly the partition's half-open window.

Direct identifiers (names, phone/email, national-ID and passport numbers,
dates of birth) are dropped as records stream through — only an analytical,
non-identifying subset is written to the raw bucket.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

BASE_URL = "https://cbs.redcross.or.ke/api/integration/v1/"

# earliest data to load; the partition start_date in defs.yaml must match
INITIAL_VALUE = "2026-06-01T00:00:00.000Z"


def _client() -> RESTClient:
    """Build a RESTClient at run time so `dg check defs` (import only) never
    needs the API key. page:1..pages pagination is shared by both resources."""
    return RESTClient(
        base_url=BASE_URL,
        headers={"x-api-key": dlt.secrets["datasources.cbs.api_key"]},
        paginator=PageNumberPaginator(base_page=1, total_path="pages"),
    )


def map_report(r: dict) -> dict:
    """Reshape a report to a flat, non-identifying subset (drops reporterName,
    reporterPhone, and the free-text description)."""
    return {
        "id": r.get("id"),
        "case_number": r.get("caseNumber"),
        "project_id": r.get("projectId"),
        "project_title": r.get("projectTitle"),
        "event_name": r.get("eventName"),
        "event_category": r.get("eventCategory"),
        "region_name": r.get("regionName"),
        "no_of_cases": r.get("noOfCases"),
        "status": r.get("status"),
        "location_name": r.get("locationName"),
        "longitude": r.get("longitude"),
        "latitude": r.get("latitude"),
        "source": r.get("source"),
        "current_escalation_level": r.get("currentEscalationLevel"),
        "date": r.get("date"),
        "created_at": r.get("createdAt"),
        "updated_at": r.get("updatedAt"),
    }


def map_screening(s: dict) -> dict:
    """Reshape a screening to a flat, non-identifying subset (drops fullName,
    dateOfBirth, identifierType/Value, phone, email and screenedBy; keeps the
    travel-history and location fields that matter epidemiologically)."""
    return {
        "id": s.get("id"),
        "system_id": s.get("systemId"),
        "project_id": s.get("projectId"),
        "project_title": s.get("projectTitle"),
        "sex": s.get("sex"),
        "nationality": s.get("nationality"),
        "travelling_from": s.get("travellingFrom"),
        "passed_through": s.get("passedThrough"),
        "screening_point_name": s.get("screeningPointName"),
        "screening_point_type": s.get("screeningPointType"),
        "region_name": s.get("regionName"),
        "location_name": s.get("locationName"),
        "longitude": s.get("longitude"),
        "latitude": s.get("latitude"),
        "source": s.get("source"),
        "screening_time": s.get("screeningTime"),
        "created_at": s.get("createdAt"),
        "updated_at": s.get("updatedAt"),
    }


@dlt.source(name="cbs", max_table_nesting=0)
def cbs_source():
    @dlt.resource(name="reports", primary_key="id", write_disposition="append")
    def reports(
        report_date=dlt.sources.incremental("date", initial_value=INITIAL_VALUE),
    ):
        client = _client()
        params = {"limit": 500, "fromDate": report_date.last_value[:10]}
        if report_date.end_value:
            params["toDate"] = report_date.end_value[:10]
        for page in client.paginate("reports", params=params):
            yield [map_report(r) for r in page]

    @dlt.resource(name="screenings", primary_key="id", write_disposition="append")
    def screenings(
        screening_time=dlt.sources.incremental(
            "screening_time", initial_value=INITIAL_VALUE
        ),
    ):
        client = _client()
        params = {"limit": 500, "fromDate": screening_time.last_value[:10]}
        if screening_time.end_value:
            params["toDate"] = screening_time.end_value[:10]
        for page in client.paginate("screenings", params=params):
            yield [map_screening(s) for s in page]

    return reports, screenings


source = cbs_source()

pipeline = dlt.pipeline(
    pipeline_name="cbs",
    destination="filesystem",
    dataset_name="cbs_raw",
)
