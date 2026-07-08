import dagster as dg

mdharura_sync_job = dg.define_asset_job(
    name="mdharura_sync_job",
    selection=dg.AssetSelection.groups("mdharura"),
    description="Loads m-Dharura signals for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 06:00 UTC for the previous day
sync_mdharura_signals_daily = dg.build_schedule_from_partitioned_job(
    mdharura_sync_job,
    hour_of_day=6,
    name="sync_mdharura_signals_daily",
    description="Syncs the previous day's signals from m-Dharura every day at 06:00 UTC",
)

lims_sync_job = dg.define_asset_job(
    name="lims_sync_job",
    selection=dg.AssetSelection.groups("lims"),
    description="Loads LIMS results for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 03:00 UTC for the previous day
sync_lims_results_daily = dg.ScheduleDefinition(
    job=lims_sync_job,
    cron_schedule="0 3 * * *",
    name="sync_lims_results_daily",
    description="Syncs results from LIMS every day at 03:00 UTC",
)

uhai_sync_job = dg.define_asset_job(
    name="uhai_sync_job",
    selection=dg.AssetSelection.groups("uhai"),
    description="Loads Uhai traveler screenings for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 04:00 UTC for the previous day
sync_uhai_screenings_daily = dg.build_schedule_from_partitioned_job(
    uhai_sync_job,
    hour_of_day=4,
    name="sync_uhai_screenings_daily",
    description="Syncs the previous day's Uhai traveler screenings every day at 04:00 UTC",
)
