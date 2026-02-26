# Shared Memory Transport Parser - Detailed Technical Flow

## 1. Goal

This document describes the final parser implementation that uses:

1. Streaming CSV parsing
2. Two-process parallel execution
3. Shared-memory transport for worker results
4. Automatic fallback to file-based transport when shared memory is unavailable

The objective is to minimize end-to-end parse time while staying within the benchmark constraints (2 vCPU, 1.5GB RAM, JIT off).

## 2. Constraints and Environment

The implementation is designed for the challenge constraints:

1. Input rows: `URL,timestamp`
2. Output: pretty-printed JSON
3. Correctness: byte-level match with expected test fixture
4. Runtime environment:
   - Container CPU limit: `2.0`
   - Container memory limit: `1536m`
   - PHP memory limit: `1280M`
   - JIT disabled

Relevant files:

1. `app/Parser.php`
2. `docker-compose.yml`
3. `docker/php/conf.d/99-benchmark.ini`

## 3. High-Level Data Flow

### 3.1 Parse entry point

`Parser::parse()` performs:

1. Optional GC disable
2. Parse input into `counts[path][dayKey]`
3. Sort day keys per path (`ksort(SORT_NUMERIC)`)
4. Serialize output via `json_encode(..., JSON_PRETTY_PRINT)`

### 3.2 Strategy selection

`parseWithBestStrategy()`:

1. Reads file size
2. Runs single-process path for small files or missing `pcntl`
3. Runs parallel path for large files (`>= 64MB`)

### 3.3 Parallel transport selection

`parseParallel()`:

1. If SysV shared memory functions exist (`shm_attach`, `shm_put_var`), uses shared-memory transport
2. Otherwise falls back to file transport (`parseParallelFile()`)

This keeps the implementation robust across environments.

## 4. Parallel Shared-Memory Flow (Primary)

For large files:

1. Split input into byte ranges aligned to newlines (`calculateRanges()`)
2. Fork exactly 2 workers
3. Each worker parses its range independently (`parseRange()`)
4. Each worker writes partial aggregation into shared memory (`shm_put_var`)
5. Parent waits all workers (`pcntl_wait` loop)
6. Parent reads worker partials from shared memory (`shm_get_var`)
7. Parent merges partial maps
8. Parent removes and detaches shared-memory segment

Shared memory segment size is fixed at `512MB`.

## 5. File Transport Fallback

If shared memory is unavailable or attach fails:

1. Workers serialize partial maps (`igbinary` if available, else `serialize`)
2. Workers write partial payloads to temp files
3. Parent reads each payload and merges
4. Temp files are cleaned in `finally`

This path is functionally equivalent to shared-memory transport, only slower due to extra I/O and serialization overhead.

## 6. Row Parsing Internals

`parseRange()` uses fixed-size binary streaming and a carry buffer:

1. Read chunk (`67,108,864` bytes)
2. Append carry from previous chunk
3. Scan newline boundaries
4. For each complete row:
   - Find comma
   - Extract URI once
   - Resolve path via:
     - fast host prefix path slicing for `https://stitcher.io`
     - fallback `parse_url(..., PHP_URL_PATH)`
   - Convert date bytes to numeric key `YYYYMMDD` without `DateTime`
   - Increment counter in nested map

Per-range structures:

1. `paths` (pid -> path)
2. `uriToPid` cache
3. `pathToPid` cache
4. `countsByPid[pid][dayKey]`

## 7. Why Shared Memory Can Improve Throughput

Compared with file transport, shared memory may reduce:

1. Worker-to-parent disk writes
2. Parent disk reads of payloads
3. Temp-file lifecycle overhead

In this workload, the gain is real but moderate (not an order of magnitude), because most time is still spent in parsing and counting.

## 8. Hardware Compliance

The implementation respects benchmark constraints:

1. CPU: max 2 workers (matches 2 vCPU target)
2. Container memory cap: 1.5GB
3. PHP process memory cap: 1280MB
4. JIT disabled
5. No FFI used

Empirical confirmation:

1. Validation passes
2. 100M parse runs complete successfully in constrained container without OOM kills

## 9. Stability and Safety Notes

1. Shared memory path is guarded by capability checks and attach checks
2. Fallback path guarantees functional continuity
3. All temporary resources are cleaned in `finally`
4. Worker failures are detected and surfaced via exceptions

## 10. Benchmarks Observed (100M parse-only, server-like container)

Recent runs of final shared-memory parser:

1. `22.605s` parser time (`23.018s` wall)
2. `23.969s` parser time (`24.319s` wall)

These numbers are in the same performance band as the best local runs and show stable completion under the hardware limits.

## 11. Tradeoffs

Pros:

1. Faster transport path in many runs
2. Maintains correctness and deterministic output
3. Keeps fallback safety

Cons:

1. More complex than file-only transport
2. SysV shared memory APIs add operational complexity
3. Performance gain depends on host-level I/O and kernel behavior

## 12. Future Extreme Paths (Optional)

If further speed is required beyond this design:

1. Dense shared-memory counter tables (pathId/dayId) with integer packing
2. Native extension implementation for parse loop and merge logic
3. SIMD-assisted parsing in native code

These can produce larger gains but significantly increase complexity and maintenance cost.
