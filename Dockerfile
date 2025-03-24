FROM ghcr.io/astral-sh/uv:python3.13-bookworm

WORKDIR /app
COPY ./uv.lock .
COPY ./pyproject.toml .
COPY ./README.md .
COPY ./src  ./src
COPY ./pipelines ./pipelines/

ENTRYPOINT ["sh"]
