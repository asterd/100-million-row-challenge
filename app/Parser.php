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

        [$dateIds, $dates, $dateCount] = $this->buildDateMap();
        [$pathIds, $paths, $pathCount] = $this->discoverPaths($inputPath, $fileSize, $dateCount);

        // Detect available CPU cores for optimal parallelism.
        // M1 Mac Mini (benchmark server) has 8 performance cores.
        $workers = $this->detectCores();

        if (
            class_exists(\parallel\Runtime::class)
            && $fileSize > self::DISCOVER_SIZE
            && $workers > 1
        ) {
            $this->parseParallel(
                $inputPath, $outputPath, $fileSize,
                $pathIds, $paths, $pathCount,
                $dateIds, $dates, $dateCount,
                $workers,
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
                $dateIds, $dates, $dateCount,
                min($workers, 8),
            );
            return;
        }

        $counts = $this->parseRange($inputPath, 0, $fileSize, $pathIds, $dateIds, $pathCount, $dateCount);
        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    private function detectCores(): int
    {
        // Try nproc first (Linux/macOS with coreutils)
        if (function_exists('shell_exec')) {
            $n = (int) shell_exec('nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null');
            if ($n > 0) {
                return min($n, 16);
            }
        }
        // /proc/cpuinfo fallback
        if (is_readable('/proc/cpuinfo')) {
            $c = substr_count((string) file_get_contents('/proc/cpuinfo'), "\nprocessor\t:");
            if ($c > 0) return min($c, 16);
        }
        return 4; // safe default for M1
    }

    /** @return array{0: array<string,int>, 1: array<int,string>, 2:int} */
    private function buildDateMap(): array
    {
        $dateIds   = [];
        $dates     = [];
        $dateCount = 0;

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
                    $key               = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key]     = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        return [$dateIds, $dates, $dateCount];
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
        $discoverSize = min($fileSize, self::DISCOVER_SIZE);
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
     * Parallel path using ext-parallel (true threads, zero IPC file I/O).
     * Each Runtime gets its own thread; Future::value() returns the result array
     * directly — no pack/unpack, no /dev/shm writes.
     *
     * @param array<string,int> $pathIds
     * @param array<int,string> $paths
     * @param array<string,int> $dateIds
     * @param array<int,string> $dates
     */
    private function parseParallel(
        string $inputPath,
        string $outputPath,
        int    $fileSize,
        array  $pathIds,
        array  $paths,
        int    $pathCount,
        array  $dateIds,
        array  $dates,
        int    $dateCount,
        int    $workers,
    ): void {
        // Split file into $workers equal byte-aligned slices.
        $boundaries = $this->buildBoundaries($inputPath, $fileSize, $workers);

        $autoloader = $this->findAutoloader();

        // Closure that each thread will run — must be static, no $this.
        $task = static function (
            string $autoloader,
            string $inputPath,
            int    $start,
            int    $end,
            array  $pathIds,
            array  $dateIds,
            int    $pathCount,
            int    $dateCount,
            int    $readChunk,
            int    $urlPrefixLen,
        ): array {
            require_once $autoloader;
            gc_disable();

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
                    $dateKey  = substr($chunk, $commaPos + 3, 8);
                    $counts[$pathIds[$slug] + $dateIds[$dateKey]]++;
                    $slugStart = $nlPos + 26;
                }
            }

            fclose($handle);
            return $counts;
        };

        // Launch workers - 1 threads (last slice runs in main thread).
        $futures = [];
        for ($w = 0; $w < $workers - 1; $w++) {
            $runtime    = new \parallel\Runtime($autoloader);
            $futures[$w] = $runtime->run(
                $task,
                $autoloader,
                $inputPath,
                $boundaries[$w],
                $boundaries[$w + 1],
                $pathIds,
                $dateIds,
                $pathCount,
                $dateCount,
                self::READ_CHUNK,
                self::URL_PREFIX_LEN,
            );
        }

        // Main thread handles last slice while workers run.
        $counts = $this->parseRange(
            $inputPath,
            $boundaries[$workers - 1],
            $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        // Collect and merge.
        foreach ($futures as $future) {
            $partial = $future->value();
            $len     = count($counts);
            for ($i = 0; $i < $len; $i++) {
                $counts[$i] += $partial[$i];
            }
        }

        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /**
     * Fallback: pcntl_fork. IPC via /dev/shm pack/unpack.
     *
     * @param array<string,int> $pathIds
     * @param array<int,string> $paths
     * @param array<string,int> $dateIds
     * @param array<int,string> $dates
     */
    private function parseParallelFork(
        string $inputPath,
        string $outputPath,
        int    $fileSize,
        array  $pathIds,
        array  $paths,
        int    $pathCount,
        array  $dateIds,
        array  $dates,
        int    $dateCount,
        int    $workers,
    ): void {
        $boundaries = $this->buildBoundaries($inputPath, $fileSize, $workers);

        $tmpDir   = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid    = getmypid();
        $children = [];

        for ($w = 0; $w < $workers - 1; $w++) {
            $tmpFile = "{$tmpDir}/p100m_{$myPid}_{$w}";
            $pid     = pcntl_fork();

            if ($pid === 0) {
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                $payload = pack('V*', ...$wCounts);
                file_put_contents($tmpFile, $payload);
                exit(0);
            }

            if ($pid < 0) throw new RuntimeException('Fork failed');

            $children[] = ['pid' => $pid, 'start' => $boundaries[$w], 'end' => $boundaries[$w + 1], 'tmpFile' => $tmpFile];
        }

        $counts = $this->parseRange(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        foreach ($children as $child) {
            pcntl_waitpid($child['pid'], $status);
            $childOk = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;
            $payload  = $childOk ? file_get_contents($child['tmpFile']) : false;
            @unlink($child['tmpFile']);

            if ($payload !== false && $payload !== '') {
                $wCounts = unpack('V*', $payload);
                if (is_array($wCounts)) {
                    $j   = 1;
                    $len = count($counts);
                    for ($i = 0; $i < $len; $i++) {
                        $counts[$i] += $wCounts[$j++];
                    }
                    continue;
                }
            }

            $fallback = $this->parseRange(
                $inputPath, $child['start'], $child['end'],
                $pathIds, $dateIds, $pathCount, $dateCount,
            );
            $len = count($counts);
            for ($i = 0; $i < $len; $i++) {
                $counts[$i] += $fallback[$i];
            }
        }

        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /**
     * Build byte boundaries aligned to newlines.
     * @return array<int,int>
     */
    private function buildBoundaries(string $inputPath, int $fileSize, int $workers): array
    {
        $boundaries = [0];
        $bh         = fopen($inputPath, 'rb');
        if ($bh === false) throw new RuntimeException("Cannot open: {$inputPath}");

        for ($i = 1; $i < $workers; $i++) {
            fseek($bh, (int) ($fileSize * $i / $workers));
            fgets($bh);
            $boundaries[] = (int) ftell($bh);
        }

        fclose($bh);
        $boundaries[] = $fileSize;
        return $boundaries;
    }

    /**
     * Core hot loop — unchanged from the original proven implementation.
     *
     * @param array<string,int> $pathIds
     * @param array<string,int> $dateIds
     * @return array<int,int>
     */
    private function parseRange(
        string $inputPath,
        int    $start,
        int    $end,
        array  $pathIds,
        array  $dateIds,
        int    $pathCount,
        int    $dateCount,
    ): array {
        $counts = array_fill(0, $pathCount * $dateCount, 0);

        $handle = fopen($inputPath, 'rb');
        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead   = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
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

            $slugStart = self::URL_PREFIX_LEN;

            while ($slugStart < $lastNl) {
                $nlPos    = strpos($chunk, "\n", $slugStart);
                $commaPos = $nlPos - 26;
                $slug     = substr($chunk, $slugStart, $commaPos - $slugStart);
                $dateKey  = substr($chunk, $commaPos + 3, 8);
                $counts[$pathIds[$slug] + $dateIds[$dateKey]]++;
                $slugStart = $nlPos + 26;
            }
        }

        fclose($handle);
        return $counts;
    }

    private function findAutoloader(): string
    {
        $dir = __DIR__;
        for ($i = 0; $i < 6; $i++) {
            $f = $dir . '/vendor/autoload.php';
            if (file_exists($f)) return $f;
            $dir = dirname($dir);
        }
        return dirname(__DIR__) . '/vendor/autoload.php';
    }

    /**
     * @param array<int,string> $paths
     * @param array<int,string> $dates
     * @param array<int,int>    $counts
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
                if ($count === 0) continue;
                $dateEntries[] = $datePrefixes[$d] . $count;
            }

            if ($dateEntries === []) continue;

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
