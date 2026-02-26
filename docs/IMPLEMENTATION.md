# 100M Row Challenge - Implementazione Finale

## 1) Vincoli rispettati

1. Input CSV: `URL,timestamp ISO8601`.
2. Output JSON pretty-printed.
3. Key root = URL path.
4. Conteggi per giorno (`YYYY-MM-DD`) ordinati in modo crescente.
5. Compatibile con `data:validate` (confronto byte-a-byte).

## 2) Strategia finale in `app/Parser.php`

1. Streaming binario a chunk da 64MB (`fread`) con gestione `carry` per righe spezzate.
2. Fast-path su host noto `https://stitcher.io` con slicing diretto della path.
3. Cache `uri -> pid` e `path -> pid` per ridurre lookup/allocazioni ripetute nel loop caldo.
4. Parsing data via byte arithmetic in `YYYYMMDD` (no `DateTime`, no parsing pesante).
5. Parallelismo deterministico con `pcntl_fork`:
   - 2 worker fissi su file grandi (>=64MB)
   - split per range allineati a newline
   - merge finale stabile dei risultati parziali
6. Serializzazione output con `json_encode(..., JSON_PRETTY_PRINT)`.

## 3) Scelte di ottimizzazione

1. Niente varianti runtime del parser (versione unica, pulita).
2. Niente writer JSON manuale: in benchmark reali `json_encode` è risultato più veloce/stabile.
3. `formatDayKey` con `intdiv` + cache stringhe giorno.
4. Merge parziali in streaming per limitare picchi RAM.

## 4) Ambiente benchmark

1. Docker con PHP 8.5 CLI.
2. Limiti server-like: 2 vCPU, 1.5GB RAM.
3. JIT disabilitato, opcache CLI attivo.

## 5) Risultati recenti (Docker server-like)

Run effettuati con parser finale:

1. Validazione:
   - `1772097113,main,0.0013120174407959` -> Validation passed
2. Parse-only 100M:
   - `1772097130,main,24.591319084167` (wall `25.039s`)
   - `1772097164,main,25.365540981293` (wall `25.753s`)
3. End-to-end 100M (generate + parse):
   - generate `19.330s`
   - parse `26.087s`
4. Tuning ulteriore (solo I/O chunk size):
   - 8MB: `27.920s` wall (peggiora)
   - 32MB: `23.074s` / `23.927s` wall
   - 64MB: `23.560s` / `22.480s` wall (miglior risultato osservato)

## 6) Nota pratica

Il tempo finale dipende da I/O disco, stato cache filesystem e contesa CPU al momento del run.

## 7) Detailed Shared-Memory Flow (English)

For a full technical breakdown of the final shared-memory transport implementation, see:

- `docs/SHARED_MEMORY_TRANSPORT_DETAILED.md`
