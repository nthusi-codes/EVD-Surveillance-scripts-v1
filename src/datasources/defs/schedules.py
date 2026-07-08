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
