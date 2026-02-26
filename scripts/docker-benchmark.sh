#!/usr/bin/env bash
set -euo pipefail

docker compose run --rm \
  -e BENCHMARK_INPUT \
  -e BENCHMARK_OUTPUT \
  -e BENCHMARK_EXPECTED \
  -e BENCHMARK_NAME \
  -e STORE_RESULT \
  php-benchmark \
  bash -lc "./scripts/benchmark-server-like.sh"
