"""lims: load data from <API> into MinIO.

See docs/developer-walkthrough.md for the step-by-step guide.
"""

import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source(
    {
        "client": {
            "base_url": "https://lab-staging-api.nphl.go.ke",
            "headers": {"x-api-key": dlt.secrets["datasources.lims.api_token"]},
        },
        "resources": [
            {
                "name": "records",
                "primary_key": "specimenIdentifier",
                "write_disposition": "append",
                "endpoint": {
                    "path": "list",
                    "params": {"limit": 50},
                    "data_selector": "data",
                },
            },
        ],
    },
    name="lims",
    max_table_nesting=0,
)

pipeline = dlt.pipeline(
    pipeline_name="lims",
    destination="filesystem",
    dataset_name="lims_raw",
)
