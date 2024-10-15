shared_params = {
    "seed": 42,
    "horizon": 14,
    "priors": {
        "rt": {
            "mean": 1.0,
            "sd": 0.2
        },
        "gp": {
            "alpha_sd": 0.01
        }
    },
    "sampler_opts": {
        "cores": 4,
        "chains": 4,
        "adapt_delta": 0.99,
        "max_treedepth": 12
    }
}
