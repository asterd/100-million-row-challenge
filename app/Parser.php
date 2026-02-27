<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;
use RuntimeException;

final class Parser
{
    private const int READ_CHUNK     = 33_554_432; // 32 MB
    private const int DISCOVER_SIZE  =  8_388_608; //  8 MB
    private const int URL_PREFIX_LEN = 25;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        if (! is_file($inputPath)) {
            throw new RuntimeException("Input file does not exist: {$inputPath}");
        }

        $fileSize = filesize($inputPath);

        if ($fileSize === false) {
            throw new RuntimeException("Unable to read input file size: {$inputPath}");
        }

        if ($fileSize === 0) {
            file_put_contents($outputPath, "{}\n");
            return;
        }

        [$dateOffsets, $dates, $dateCount] = $this->buildDateMap();
        [$pathIds, $paths, $pathCount]     = $this->discoverPaths($inputPath, $fileSize, $dateCount);

        $useParallel = class_exists(\parallel\Runtime::class)
            && $fileSize > self::DISCOVER_SIZE;

        if ($useParallel) {
            $this->parseParallel(
                $inputPath, $outputPath, $fileSize,
                $pathIds, $paths, $pathCount,
                $dateOffsets, $dates, $dateCount,
            );
            return;
        }

        if (
            function_exists('pcntl_fork')
            && function_exists('pcntl_waitpid')
            && $fileSize > self::DISCOVER_SIZE
        ) {
            $this->parseParallelFork(
                $inputPath, $outputPath, $fileSize,
                $pathIds, $paths, $pathCount,
                $dateOffsets, $dates, $dateCount,
            );
            return;
        }

        $counts = $this->parseRange($inputPath, 0, $fileSize, $pathIds, $dateOffsets, $pathCount, $dateCount);
        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /**
     * Costruisce due strutture per le date:
     * - $dateOffsets: array indicizzato per chiave intera (yy*400 + mm*32 + dd) → indice sequenziale
     *   Permette di calcolare l'indice con ord() sui byte del chunk, senza hash lookup su stringa.
     * - $dates: array indicizzato sequenziale → stringa "YY-MM-DD" per writeJson
     *
     * @return array{0: array<int,int>, 1: array<int,string>, 2: int}
     */
    private function buildDateMap(): array
    {
        $dateOffsets = [];
        $dates       = [];
        $dateCount   = 0;

        for ($y = 20; $y <= 26; $y++) {
            $yStr = ($y < 10 ? '0' : '') . $y;

            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };

                $mStr  = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';

                for ($d = 1; $d <= $maxD; $d++) {
                    $intKey              = $y * 400 + $m * 32 + $d;
                    $dateOffsets[$intKey] = $dateCount;
                    $dates[$dateCount]   = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateCount++;
                }
            }
        }

        return [$dateOffsets, $dates, $dateCount];
    }

    /** @return array{0: array<string,int>, 1: array<int,string>, 2:int} */
    private function discoverPaths(string $inputPath, int $fileSize, int $dateCount): array
    {
        $pathIds   = [];
        $paths     = [];
        $pathCount = 0;

        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);
        $discoverSize = $fileSize > self::DISCOVER_SIZE ? self::DISCOVER_SIZE : $fileSize;
        $chunk        = fread($handle, $discoverSize);
        fclose($handle);

        if (is_string($chunk) && $chunk !== '') {
            $lastNl = strrpos($chunk, "\n");

            if (is_int($lastNl)) {
                $slugStart = self::URL_PREFIX_LEN;

                while ($slugStart < $lastNl) {
                    $commaPos = strpos($chunk, ',', $slugStart);
                    $slug     = substr($chunk, $slugStart, $commaPos - $slugStart);

                    if (! isset($pathIds[$slug])) {
                        $pathIds[$slug]    = $pathCount * $dateCount;
                        $paths[$pathCount] = $slug;
                        $pathCount++;
                    }

                    $slugStart = $commaPos + 52;
                }
            }
        }

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::URL_PREFIX_LEN);

            if (! isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount * $dateCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        return [$pathIds, $paths, $pathCount];
    }

    /**
     * Versione con ext-parallel: thread veri, autoloader condiviso, zero IPC I/O.
     * Il valore di ritorno di Future::value() porta il $counts direttamente in memoria
     * senza pack/unpack/file, eliminando ~50ms di overhead IPC per worker.
     *
     * @param array<string, int> $pathIds
     * @param array<int, string> $paths
     * @param array<int, int>    $dateOffsets
     * @param array<int, string> $dates
     */
    private function parseParallel(
        string $inputPath,
        string $outputPath,
        int    $fileSize,
        array  $pathIds,
        array  $paths,
        int    $pathCount,
        array  $dateOffsets,
        array  $dates,
        int    $dateCount,
    ): void {
        // Trova il boundary a metà file allineato al newline
        $bh = fopen($inputPath, 'rb');
        if ($bh === false) throw new RuntimeException("Unable to open: {$inputPath}");
        fseek($bh, (int) ($fileSize / 2));
        fgets($bh);
        $mid = ftell($bh);
        fclose($bh);

        // Trova vendor autoloader per il Runtime thread
        $autoloader = $this->findAutoloader();

        $runtime = new \parallel\Runtime($autoloader);

        // Thread worker: prima metà del file
        $future = $runtime->run(
            static function (
                string $inputPath,
                int    $start,
                int    $end,
                array  $pathIds,
                array  $dateOffsets,
                int    $pathCount,
                int    $dateCount,
                int    $readChunk,
                int    $urlPrefixLen,
            ): array {
                gc_disable();
                return \App\Parser::parseChunk(
                    $inputPath, $start, $end,
                    $pathIds, $dateOffsets,
                    $pathCount, $dateCount,
                    $readChunk, $urlPrefixLen,
                );
            },
            $inputPath, 0, $mid,
            $pathIds, $dateOffsets,
            $pathCount, $dateCount,
            self::READ_CHUNK, self::URL_PREFIX_LEN,
        );

        // Padre: seconda metà in parallelo
        $counts = $this->parseRange($inputPath, $mid, $fileSize, $pathIds, $dateOffsets, $pathCount, $dateCount);

        // Merge senza pack/unpack — array PHP diretto
        $workerCounts = $future->value();
        $len = count($counts);
        for ($i = 0; $i < $len; $i++) {
            $counts[$i] += $workerCounts[$i];
        }

        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /**
     * Fallback pcntl_fork (2 worker, ottimale per 2 vCPU).
     *
     * @param array<string, int> $pathIds
     * @param array<int, string> $paths
     * @param array<int, int>    $dateOffsets
     * @param array<int, string> $dates
     */
    private function parseParallelFork(
        string $inputPath,
        string $outputPath,
        int    $fileSize,
        array  $pathIds,
        array  $paths,
        int    $pathCount,
        array  $dateOffsets,
        array  $dates,
        int    $dateCount,
    ): void {
        $bh = fopen($inputPath, 'rb');
        if ($bh === false) throw new RuntimeException("Unable to open: {$inputPath}");
        fseek($bh, (int) ($fileSize / 2));
        fgets($bh);
        $mid = ftell($bh);
        fclose($bh);

        $tmpDir  = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $tmpFile = $tmpDir . '/p100m_' . getmypid() . '_0';
        $pid     = pcntl_fork();

        if ($pid === 0) {
            $wCounts = $this->parseRange($inputPath, 0, $mid, $pathIds, $dateOffsets, $pathCount, $dateCount);
            $payload = pack('V*', ...$wCounts);
            file_put_contents($tmpFile, $payload);
            exit(0);
        }

        if ($pid < 0) {
            throw new RuntimeException('Fork failed');
        }

        // Padre: seconda metà
        $counts = $this->parseRange($inputPath, $mid, $fileSize, $pathIds, $dateOffsets, $pathCount, $dateCount);

        pcntl_waitpid($pid, $status);
        $childOk = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;
        $payload = $childOk ? file_get_contents($tmpFile) : false;
        @unlink($tmpFile);

        if ($payload !== false && $payload !== '') {
            $wCounts = unpack('V*', $payload);
            if (is_array($wCounts)) {
                $len = count($counts);
                $j   = 1; // unpack V* è 1-indexed
                for ($i = 0; $i < $len; $i++) {
                    $counts[$i] += $wCounts[$j++];
                }
            }
        } else {
            // Fallback: ricalcola la prima metà nel padre
            $fallback = $this->parseRange($inputPath, 0, $mid, $pathIds, $dateOffsets, $pathCount, $dateCount);
            $len      = count($counts);
            for ($i = 0; $i < $len; $i++) {
                $counts[$i] += $fallback[$i];
            }
        }

        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /**
     * Metodo statico pubblico per chiamarlo dal thread parallel (che non ha $this).
     *
     * @param array<string, int> $pathIds
     * @param array<int, int>    $dateOffsets
     * @return array<int, int>
     */
    public static function parseChunk(
        string $inputPath,
        int    $start,
        int    $end,
        array  $pathIds,
        array  $dateOffsets,
        int    $pathCount,
        int    $dateCount,
        int    $readChunk,
        int    $urlPrefixLen,
    ): array {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead   = $remaining > $readChunk ? $readChunk : $remaining;
            $chunk    = fread($handle, $toRead);
            $chunkLen = strlen($chunk);

            if ($chunkLen === 0) break;

            $remaining -= $chunkLen;
            $lastNl     = strrpos($chunk, "\n");

            if ($lastNl === false) {
                fseek($handle, -$chunkLen, SEEK_CUR);
                $remaining += $chunkLen;
                break;
            }

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $slugStart = $urlPrefixLen;

            while ($slugStart < $lastNl) {
                $nlPos    = strpos($chunk, "\n", $slugStart);
                $commaPos = $nlPos - 26;
                $slug     = substr($chunk, $slugStart, $commaPos - $slugStart);

                $o  = $commaPos + 3;
                $yy = (ord($chunk[$o])     - 48) * 10 + (ord($chunk[$o + 1]) - 48);
                $mm = (ord($chunk[$o + 3]) - 48) * 10 + (ord($chunk[$o + 4]) - 48);
                $dd = (ord($chunk[$o + 6]) - 48) * 10 + (ord($chunk[$o + 7]) - 48);

                $counts[$pathIds[$slug] + $dateOffsets[$yy * 400 + $mm * 32 + $dd]]++;

                $slugStart = $nlPos + 26;
            }
        }

        fclose($handle);
        return $counts;
    }

    /**
     * Hot loop principale — usato dal padre e dal fallback fork.
     *
     * Ottimizzazione chiave vs originale: la data non viene estratta con substr()
     * e poi cercata in una hash map string→int. Invece si usano ord() sui singoli
     * byte del chunk (già in memoria) per costruire una chiave intera e fare una
     * lookup su array di int, che PHP risolve senza hashing di stringa.
     *
     * @param array<string, int> $pathIds
     * @param array<int, int>    $dateOffsets
     * @return array<int, int>
     */
    private function parseRange(
        string $inputPath,
        int    $start,
        int    $end,
        array  $pathIds,
        array  $dateOffsets,
        int    $pathCount,
        int    $dateCount,
    ): array {
        return self::parseChunk(
            $inputPath, $start, $end,
            $pathIds, $dateOffsets,
            $pathCount, $dateCount,
            self::READ_CHUNK, self::URL_PREFIX_LEN,
        );
    }

    private function findAutoloader(): string
    {
        $dir = __DIR__;
        for ($i = 0; $i < 6; $i++) {
            $candidate = $dir . '/vendor/autoload.php';
            if (file_exists($candidate)) return $candidate;
            $dir = dirname($dir);
        }
        return dirname(__DIR__) . '/vendor/autoload.php';
    }

    /**
     * @param array<int, string> $paths
     * @param array<int, string> $dates
     * @param array<int, int>    $counts
     */
    private function writeJson(
        string $outputPath,
        array  $paths,
        int    $pathCount,
        array  $dates,
        int    $dateCount,
        array  $counts,
    ): void {
        $datePrefixes = [];

        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pathHeaders = [];

        for ($p = 0; $p < $pathCount; $p++) {
            $pathHeaders[$p] = "\n    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\": {\n";
        }

        $out = fopen($outputPath, 'wb');

        if ($out === false) {
            throw new RuntimeException("Unable to write output file: {$outputPath}");
        }

        stream_set_write_buffer($out, self::READ_CHUNK);
        fwrite($out, '{');

        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base        = $p * $dateCount;
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];

                if ($count === 0) {
                    continue;
                }

                $dateEntries[] = $datePrefixes[$d] . $count;
            }

            if ($dateEntries === []) {
                continue;
            }

            fwrite(
                $out,
                ($firstPath ? '' : ',')
                . $pathHeaders[$p]
                . implode(",\n", $dateEntries)
                . "\n    }",
            );
            $firstPath = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
