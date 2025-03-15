#!/usr/bin/env sh

set -e

for projectdir in compute_horde compute_horde_sdk executor miner validator; do
  cd "${projectdir}"
  uv sync --all-groups
  uv run ruff check --fix
  uv run ruff format
  uv run nox -s type_check lint
  cd ".."
done

uv sync --all-groups