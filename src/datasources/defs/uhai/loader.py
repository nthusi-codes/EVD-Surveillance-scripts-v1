"""Load Uhai Ebola traveler screenings from PostgreSQL into MinIO.

Uhai stores traveler Ebola screening submissions in PostgreSQL. This source
loads one Dagster partition window at a time from traveler_screenings and joins
the latest health_passes row for each screening where available.
"""

from collections.abc import Iterator
from datetime import date, datetime, time
from typing import Any

import dlt
import psycopg
from psycopg.rows import dict_row

BATCH_SIZE = 1000


def _database_url() -> str:
    return dlt.secrets["datasources.uhai.database_url"]


def _iso(value: Any) -> str:
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    return "" if value is None else str(value)


def _row_to_screening(row: dict[str, Any]) -> dict[str, Any]:
    result = row.get("risk_level") or row.get("screening_status") or ""
    identifier_number = row.get("id_number") or row.get("pass_id_number") or ""
    names = row.get("full_name") or row.get("pass_full_name") or ""

    return {
        "system_id": _iso(row.get("system_id")),
        "names": names,
        "sex": row.get("sex") or "",
        "date_of_birth": _iso(row.get("date_of_birth")),
        "system_id": _iso(row.get("system_id")),
        "nationality": row.get("nationality") or "",
        "identifier_type": row.get("id_type") or "",
        "identifier": identifier_number,
        "suspected": "yes" if row.get("risk_level") == "high_risk" else "no",
        "screening": row.get("status") or "",
        "confirmed": "",
        "died": "",
        "recovered": "",
        "tested": "",
        "result": result,
        "point_of_entry": row.get("entry_city") or "",
        "reporting_county": "",
        "reporting_sub_county": "",
        "ward": "",
        "facility_fid": "",
        "community_health_unit_chu": "",
        "reporting_date": _iso(row.get("reporting_date")),
        "reporting_time": _iso(row.get("reporting_time")),
        "created_at": _iso(row.get("created_at")),
    }


@dlt.source(name="uhai", max_table_nesting=0)
def uhai_source():
    @dlt.resource(
        name="traveler_screenings",
        primary_key="system_id",
        write_disposition="append",
    )
    def traveler_screenings(
        created_at=dlt.sources.incremental(
            "created_at", initial_value="2026-07-03T00:00:00.000Z"
        ),
    ) -> Iterator[list[dict[str, Any]]]:
        query = """
            WITH latest_health_pass AS (
                SELECT DISTINCT ON (screening_id)
                    screening_id,
                    full_name AS pass_full_name,
                    id_number AS pass_id_number,
                    screening_status
                FROM health_passes
                ORDER BY screening_id, id DESC
            )
            SELECT
                ts.id AS system_id,
                ts.full_name,
                ts.sex,
                ts.date_of_birth,
                ts.nationality,
                ts.id_type,
                ts.id_number,
                ts.risk_level,
                ts.status,
                ts.entry_city,
                ts.created_at,
                ts.created_at::date AS reporting_date,
                ts.created_at::time AS reporting_time,
                hp.pass_full_name,
                hp.pass_id_number,
                hp.screening_status
            FROM traveler_screenings ts
            LEFT JOIN latest_health_pass hp ON hp.screening_id = ts.id
            WHERE ts.created_at >= %(start)s::timestamptz
              AND (%(end)s::timestamptz IS NULL OR ts.created_at < %(end)s::timestamptz)
            ORDER BY ts.created_at, ts.id
        """
        params = {"start": created_at.last_value, "end": created_at.end_value}

        with psycopg.connect(_database_url(), row_factory=dict_row) as conn:
            with conn.cursor(name="uhai_traveler_screenings") as cursor:
                cursor.execute(query, params)
                while rows := cursor.fetchmany(BATCH_SIZE):
                    yield [_row_to_screening(row) for row in rows]

    return traveler_screenings


source = uhai_source()

pipeline = dlt.pipeline(
    pipeline_name="uhai",
    destination="filesystem",
    dataset_name="uhai_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
