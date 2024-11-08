#!/usr/bin/env bash
# Copyright 2017, Reef Technologies (reef.pl), All rights reserved.

set -e

# Create a lock file, install Python dependencies
[ -f uv.lock ] || uv lock
uv sync --group dev
