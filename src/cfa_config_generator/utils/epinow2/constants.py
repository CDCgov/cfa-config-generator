shared_params = {
    "seed": 42,
    "horizon": 14,
    "priors": {"rt": {"mean": 1.0, "sd": 0.2}, "gp": {"alpha_sd": 0.01}},
    "sampler_opts": {
        "cores": 4,
        "chains": 4,
        "iter_warmup": 5000,
        "iter_sampling": 4000,  # 1000 draws per chain
        "adapt_delta": 0.99,
        "max_treedepth": 12,
    },
    "config_version": "1.0",
    "quantile_width": [0.5, 0.95],
    "model": "EpiNow2",
}

all_states = [
    "AK",
    "AL",
    "AR",
    "AS",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
    "US",
]

nssp_states_omit = ["AS", "FM", "MH", "NP", "PR", "PW", "VI", "MO", "GU"]
all_diseases = ["COVID-19", "Influenza"]
data_sources = ["nhsn", "nssp"]

azure_storage = {
    "azure_storage_account_url": "https://cfaazurebatchprd.blob.core.windows.net/",
    "azure_container_name": "rt-epinow2-config",
    "scope_url": "https://cfaazurebatchprd.blob.core.windows.net/.default",
}

modifiable_params = [
    "parameters",
    "data",
    "seed",
    "horizon",
    "priors",
    "sampler_opts",
    "exclusions",
    "quantile_width",
    "model",
]

sample_task = {
    "job_id": "sample-job-id",
    "task_id": "sample-task-id",
    "min_reference_date": "2024-10-22",
    "max_reference_date": "2024-12-16",
    "disease": "COVID-19",
    "geo_value": "AK",
    "geo_type": "state",
    "report_date": "2024-12-17",
    "production_date": "2024-12-17",
    "parameters": {
        "as_of_date": "2024-12-17T19:50:06.000814+00:00",
        "generation_interval": {
            "path": "prod.parquet",
            "blob_storage_container": "prod-param-estimates",
        },
        "delay_interval": {
            "path": "prod.parquet",
            "blob_storage_container": "prod-param-estimates",
        },
        "right_truncation": {
            "path": "prod.parquet",
            "blob_storage_container": "prod-param-estimates",
        },
    },
    "data": {
        "path": "gold/2024-12-17.parquet",
        "blob_storage_container": "nssp-etl",
    },
    "seed": 42,
    "horizon": 14,
    "priors": {"rt": {"mean": 1.0, "sd": 0.2}, "gp": {"alpha_sd": 0.01}},
    "sampler_opts": {
        "cores": 4,
        "chains": 4,
        "iter_warmup": 5000,
        "iter_sampling": 10000,
        "adapt_delta": 0.99,
        "max_treedepth": 12,
    },
    "exclusions": {"path": None},
    "config_version": "1.0",
    "quantile_width": [0.5, 0.95],
    "model": "EpiNow2",
}
