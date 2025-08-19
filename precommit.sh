#!/usr/bin/env sh

set -e

for projectdir in compute_horde compute_horde_sdk executor miner validator facilitator; do
  cd "${projectdir}"
  uv sync --all-groups --all-extras
  uv run ruff check --fix
  uv run ruff format
  uv run nox -s lint
  cd ".."
done

# Type checking in facilitator is not a good idea
for projectdir in compute_horde compute_horde_sdk executor miner validator ; do
  cd "${projectdir}"
  uv run nox -s type_check
  cd ".."
done
