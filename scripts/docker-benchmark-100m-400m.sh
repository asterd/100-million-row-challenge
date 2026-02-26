#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-php-benchmark}"
PARSER_WORKERS="${PARSER_WORKERS:-2}"
BENCHMARK_SEED="${BENCHMARK_SEED:-1}"
SIZE_1="${SIZE_1:-100000000}"
SIZE_2="${SIZE_2:-400000000}"

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
REPORT_MD="${REPORT_MD:-data/benchmark-report-${TIMESTAMP}.md}"
REPORT_CSV="${REPORT_CSV:-data/benchmark-report-${TIMESTAMP}.csv}"

mkdir -p "$(dirname "$REPORT_MD")" "$(dirname "$REPORT_CSV")"

echo "Config:"
echo "  - Service        : ${SERVICE_NAME}"
echo "  - Parser workers : ${PARSER_WORKERS}"
echo "  - Seed           : ${BENCHMARK_SEED}"
echo "  - Pass 1 rows    : ${SIZE_1}"
echo "  - Pass 2 rows    : ${SIZE_2}"
echo "  - Report MD      : ${REPORT_MD}"
echo "  - Report CSV     : ${REPORT_CSV}"
echo

echo "Building Docker image (${SERVICE_NAME})..."
docker compose build "${SERVICE_NAME}"

echo "Running benchmark passes inside Docker..."
docker compose run --rm \
  -e PARSER_WORKERS="${PARSER_WORKERS}" \
  -e BENCHMARK_SEED="${BENCHMARK_SEED}" \
  -e SIZE_1="${SIZE_1}" \
  -e SIZE_2="${SIZE_2}" \
  -e REPORT_MD="/workspace/${REPORT_MD}" \
  -e REPORT_CSV="/workspace/${REPORT_CSV}" \
  "${SERVICE_NAME}" \
  bash -lc '
    set -euo pipefail

    now_iso() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }
    now_epoch() { date +%s.%N; }
    elapsed_s() { awk -v s="$1" -v e="$2" "BEGIN { printf \"%.3f\", (e - s) }"; }
    bytes_to_gib() { awk -v b="$1" "BEGIN { printf \"%.3f\", (b / 1073741824) }"; }

    run_pass() {
      local rows="$1"
      local label="$2"
      local input="/workspace/data/data-${label}.csv"
      local output="/workspace/data/data-${label}.json"

      rm -f "$input" "$output"

      local g_start g_end p_start p_end
      echo "[$(now_iso)] Generating dataset (${rows} rows) -> ${input}"
      g_start="$(now_epoch)"
      php ./tempest data:generate "$rows" "$input" "$BENCHMARK_SEED" --force
      g_end="$(now_epoch)"

      echo "[$(now_iso)] Parsing dataset (${rows} rows) -> ${output}"
      p_start="$(now_epoch)"
      php ./tempest data:parse "$input" "$output"
      p_end="$(now_epoch)"

      local gen_s parse_s total_s csv_bytes json_bytes csv_gib json_gib
      gen_s="$(elapsed_s "$g_start" "$g_end")"
      parse_s="$(elapsed_s "$p_start" "$p_end")"
      total_s="$(awk -v a="$gen_s" -v b="$parse_s" "BEGIN { printf \"%.3f\", (a + b) }")"

      csv_bytes="$(stat -c%s "$input")"
      json_bytes="$(stat -c%s "$output")"
      csv_gib="$(bytes_to_gib "$csv_bytes")"
      json_gib="$(bytes_to_gib "$json_bytes")"

      printf "| %s | %s | %s | %s | %s | %s |\n" "$rows" "$gen_s" "$parse_s" "$total_s" "$csv_gib" "$json_gib" >> "$REPORT_MD"
      printf "%s,%s,%s,%s,%s,%s\n" "$rows" "$gen_s" "$parse_s" "$total_s" "$csv_bytes" "$json_bytes" >> "$REPORT_CSV"

      echo "Pass ${rows}: gen=${gen_s}s parse=${parse_s}s total=${total_s}s"
    }

    echo "[$(now_iso)] Installing production dependencies"
    composer install --no-dev --prefer-dist --no-interaction

    rm -rf /workspace/.tempest
    echo "[$(now_iso)] Refreshing Tempest cache/discovery"
    php ./tempest cache:clear --force
    php ./tempest discovery:generate

    {
      echo "# Benchmark Report (Docker)"
      echo
      echo "- Date (UTC): $(now_iso)"
      echo "- Parser workers: ${PARSER_WORKERS}"
      echo "- Seed: ${BENCHMARK_SEED}"
      echo
      echo "| Rows | Generate (s) | Parse (s) | Total (s) | CSV Size (GiB) | JSON Size (GiB) |"
      echo "|---:|---:|---:|---:|---:|---:|"
    } > "$REPORT_MD"

    echo "rows,generate_s,parse_s,total_s,csv_bytes,json_bytes" > "$REPORT_CSV"

    run_pass "$SIZE_1" "100m"
    run_pass "$SIZE_2" "400m"
  '

echo
echo "Done."
echo "Report MD : ${REPORT_MD}"
echo "Report CSV: ${REPORT_CSV}"
