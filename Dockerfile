FROM ghcr.io/astral-sh/uv:python3.13-bookworm

WORKDIR /app
COPY ./uv.lock .
COPY ./pyproject.toml .
COPY ./README.md .
COPY ./src  ./src
COPY ./pipelines ./pipelines/

ENTRYPOINT ["sh"]
# ENTRYPOINT [ "uv", "run", "python", "pipelines/epinow2/generate_config_local.py" ]
