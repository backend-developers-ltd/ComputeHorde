ARG BASE_IMAGE=python:3.11-slim


FROM $BASE_IMAGE AS base-image
LABEL builder=true

RUN apt-get update && apt-get install -y git
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

COPY --from=compute-horde . /root/compute_horde

WORKDIR /root/src/
COPY pyproject.toml uv.lock ./

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1
# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

RUN --mount=type=cache,target=/root/.cache/uv \
	PDM_BUILD_SCM_VERSION=0 uv sync --frozen --no-install-project --no-dev --no-editable

COPY ./app/src/ /root/src/
COPY ./app/envs/prod/entrypoint.sh /root/src/
COPY ./app/envs/prod/gunicorn.conf.py /root/src/
COPY ./app/envs/prod/celery-entrypoint.sh /root/src/

RUN --mount=type=cache,target=/root/.cache/uv \
	PDM_BUILD_SCM_VERSION=0 uv sync --frozen --no-install-project --no-dev --no-editable

RUN ENV=prod ENV_FILL_MISSING_VALUES=1 DATABASE_URL= SECRET_KEY=dummy uv run --no-sync python3 manage.py collectstatic --no-input --clear


FROM $BASE_IMAGE AS secondary-image
LABEL builder=false

RUN apt-get update \
	&& apt-get install -y wget docker.io docker-compose \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /root/src/
ENV PYTHONUNBUFFERED=1
ENV PATH="/root/src/.venv/bin:$PATH"

COPY --from=base-image /root/src/ /root/src/

EXPOSE 8000

CMD ["./entrypoint.sh"]
