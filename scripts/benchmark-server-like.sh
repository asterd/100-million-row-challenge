#!/usr/bin/env bash
set -euo pipefail

BENCHMARK_INPUT="${BENCHMARK_INPUT:-/workspace/data/data.csv}"
BENCHMARK_OUTPUT="${BENCHMARK_OUTPUT:-/workspace/data/data.json}"
BENCHMARK_EXPECTED="${BENCHMARK_EXPECTED:-}"
BENCHMARK_NAME="${BENCHMARK_NAME:-local-benchmark}"
STORE_RESULT="${STORE_RESULT:-0}"

composer install --no-dev --prefer-dist --no-interaction

rm -rf /workspace/.tempest
php ./tempest cache:clear --force
php ./tempest discovery:generate

if [[ "${STORE_RESULT}" == "1" ]]; then
  php ./tempest data:parse "${BENCHMARK_INPUT}" "${BENCHMARK_OUTPUT}" true "${BENCHMARK_NAME}"
else
  php ./tempest data:parse "${BENCHMARK_INPUT}" "${BENCHMARK_OUTPUT}"
fi

if [[ -n "${BENCHMARK_EXPECTED}" ]]; then
  php ./tempest data:verify "${BENCHMARK_OUTPUT}" "${BENCHMARK_EXPECTED}"
fi
