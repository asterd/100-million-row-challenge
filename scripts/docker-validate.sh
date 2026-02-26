#!/usr/bin/env bash
set -euo pipefail

docker compose run --rm php-benchmark bash -lc "./scripts/validate-server-like.sh"
