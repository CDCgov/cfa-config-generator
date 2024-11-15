shared_params = {
    "seed": 42,
    "horizon": 14,
    "priors": {"rt": {"mean": 1.0, "sd": 0.2}, "gp": {"alpha_sd": 0.01}},
     "parameters": {
       "generation_interval": {
         "path": "gold/",
         "blob_storage_container": None
       },
       "delay_interval": {
         "path": None,
         "blob_storage_container": None
       },
       "right_truncation": {
         "path": None,
         "blob_storage_container": None
       }
    },
    "sampler_opts": {
        "cores": 1,
        "chains": 1,
        "iter_warmup": 50,
        "iter_sampling": 50,
        "adapt_delta": 0.99,
        "max_treedepth": 12,
    },
    "exclusions": {"path": None},
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
