shared_params = {
    "seed": 42,
    "horizon": 14,
    "priors": {"rt": {"mean": 1.0, "sd": 0.2}, "gp": {"alpha_sd": 0.01}},
    "sampler_opts": {
        "cores": 4,
        "chains": 4,
        "iter_warmup": 5000,
        "iter_sampling": 10000,  # 2500 x 4 chains
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
