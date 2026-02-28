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

        $workers = $this->detectCores();

        if (
            function_exists('pcntl_fork')
            && function_exists('pcntl_waitpid')
            && function_exists('shmop_open')
            && $fileSize > self::DISCOVER_SIZE
        ) {
            $this->parseParallelShmop(
                $inputPath, $outputPath, $fileSize,
                $pathIds, $paths, $pathCount,
                $dateIds, $dates, $dateCount,
                $workers,
            );
            return;
        }

        if (
            class_exists(\parallel\Runtime::class)
            && $fileSize > self::DISCOVER_SIZE
        ) {
            $this->parseParallelRuntime(
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
                $workers,
            );
            return;
        }

        $counts = $this->parseRange($inputPath, 0, $fileSize, $pathIds, $dateIds, $pathCount, $dateCount);
        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    private function detectCores(): int
    {
        if (function_exists('shell_exec')) {
            $n = (int) shell_exec('nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null');
            if ($n > 0) return min($n, 16);
        }
        if (is_readable('/proc/cpuinfo')) {
            $c = substr_count((string) file_get_contents('/proc/cpuinfo'), "\nprocessor\t:");
            if ($c > 0) return min($c, 16);
        }
        return 4;
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
        if ($handle === false) throw new RuntimeException("Unable to open: {$inputPath}");

        stream_set_read_buffer($handle, 0);
        $chunk = fread($handle, min($fileSize, self::DISCOVER_SIZE));
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

    /** @return array<int,int> */
    private function buildBoundaries(string $inputPath, int $fileSize, int $workers): array
    {
        $boundaries = [0];
        $bh = fopen($inputPath, 'rb');
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
     * FASTEST PATH: pcntl_fork + shmop shared memory.
     *
     * Each child writes its $counts array directly into a pre-allocated
     * shared memory segment as a packed binary string. The parent reads
     * all segments simultaneously after waitpid — zero filesystem I/O,
     * zero pack/unpack round-trips through /dev/shm files.
     *
     * Layout of each shmop segment: N * 4 bytes (uint32 little-endian).
     * Children write with pack('V*'), parent reads with unpack('V*').
     * The segment is pre-allocated before fork so children only write.
     *
     * @param array<string,int> $pathIds
     * @param array<int,string> $paths
     * @param array<string,int> $dateIds
     * @param array<int,string> $dates
     */
    private function parseParallelShmop(
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
        $total       = $pathCount * $dateCount;
        $segmentSize = $total * 4; // 4 bytes per uint32
        $boundaries  = $this->buildBoundaries($inputPath, $fileSize, $workers);

        // Pre-allocate shared memory segments — one per child worker.
        // IPC key: use a unique key per worker based on PID to avoid collisions.
        $myPid    = getmypid();
        $shmKeys  = [];
        $shmIds   = [];

        for ($w = 0; $w < $workers - 1; $w++) {
            // Use ftok-style unique integer key
            $key = 0x100000 + ($myPid % 0xFFFF) * 16 + $w;
            // SHM_OPEN: 0666 create new, size = segmentSize
            $shmId = shmop_open($key, 'n', 0666, $segmentSize);
            if ($shmId === false) {
                // Key collision — try to delete and recreate
                $shmId = shmop_open($key, 'c', 0666, $segmentSize);
            }
            $shmKeys[$w] = $key;
            $shmIds[$w]  = $shmId;
        }

        $children = [];

        for ($w = 0; $w < $workers - 1; $w++) {
            $pid = pcntl_fork();

            if ($pid === 0) {
                // CHILD: parse slice, write result directly to shared memory
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                // pack to binary and write to shm segment
                $payload = pack('V*', ...$wCounts);
                shmop_write($shmIds[$w], $payload, 0);
                exit(0);
            }

            if ($pid < 0) throw new RuntimeException('Fork failed');
            $children[$w] = $pid;
        }

        // Parent parses last slice while children work in parallel
        $counts = $this->parseRange(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        // Wait for all children and merge from shared memory
        foreach ($children as $w => $pid) {
            pcntl_waitpid($pid, $status);

            $shmId = $shmIds[$w];
            if ($shmId !== false) {
                $childOk = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;
                if ($childOk) {
                    $payload = shmop_read($shmId, 0, $segmentSize);
                    if ($payload !== false) {
                        $wCounts = unpack('V*', $payload);
                        if (is_array($wCounts)) {
                            // Merge: tight for-loop is faster than foreach + iterator
                            $j   = 1; // unpack is 1-indexed
                            for ($i = 0; $i < $total; $i++) {
                                $counts[$i] += $wCounts[$j++];
                            }
                        }
                    }
                }
                shmop_delete($shmId);
                shmop_close($shmId);

                if (! $childOk) {
                    // Fallback: re-parse failed worker's slice in parent
                    $fallback = $this->parseRange(
                        $inputPath, $boundaries[$w], $boundaries[$w + 1],
                        $pathIds, $dateIds, $pathCount, $dateCount,
                    );
                    for ($i = 0; $i < $total; $i++) {
                        $counts[$i] += $fallback[$i];
                    }
                }
            }
        }

        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /**
     * Second path: ext-parallel (true threads, no fork overhead).
     *
     * @param array<string,int> $pathIds
     * @param array<int,string> $paths
     * @param array<string,int> $dateIds
     * @param array<int,string> $dates
     */
    private function parseParallelRuntime(
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
        $autoloader = $this->findAutoloader();

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

        $futures = [];
        for ($w = 0; $w < $workers - 1; $w++) {
            $runtime     = new \parallel\Runtime($autoloader);
            $futures[$w] = $runtime->run(
                $task,
                $autoloader, $inputPath,
                $boundaries[$w], $boundaries[$w + 1],
                $pathIds, $dateIds, $pathCount, $dateCount,
                self::READ_CHUNK, self::URL_PREFIX_LEN,
            );
        }

        $counts = $this->parseRange(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        $total = $pathCount * $dateCount;
        foreach ($futures as $future) {
            $partial = $future->value();
            for ($i = 0; $i < $total; $i++) {
                $counts[$i] += $partial[$i];
            }
        }

        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /**
     * Fallback: pcntl_fork with /dev/shm file IPC.
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
        $tmpDir     = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid      = getmypid();
        $children   = [];
        $total      = $pathCount * $dateCount;

        for ($w = 0; $w < $workers - 1; $w++) {
            $tmpFile = "{$tmpDir}/p100m_{$myPid}_{$w}";
            $pid     = pcntl_fork();

            if ($pid === 0) {
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                file_put_contents($tmpFile, pack('V*', ...$wCounts));
                exit(0);
            }

            if ($pid < 0) throw new RuntimeException('Fork failed');
            $children[$w] = ['pid' => $pid, 'file' => $tmpFile,
                             'start' => $boundaries[$w], 'end' => $boundaries[$w + 1]];
        }

        $counts = $this->parseRange(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        foreach ($children as $w => $child) {
            pcntl_waitpid($child['pid'], $status);
            $ok      = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;
            $payload = $ok ? file_get_contents($child['file']) : false;
            @unlink($child['file']);

            if ($payload !== false && $payload !== '') {
                $wCounts = unpack('V*', $payload);
                if (is_array($wCounts)) {
                    $j = 1;
                    for ($i = 0; $i < $total; $i++) {
                        $counts[$i] += $wCounts[$j++];
                    }
                    continue;
                }
            }

            $fallback = $this->parseRange(
                $inputPath, $child['start'], $child['end'],
                $pathIds, $dateIds, $pathCount, $dateCount,
            );
            for ($i = 0; $i < $total; $i++) {
                $counts[$i] += $fallback[$i];
            }
        }

        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /**
     * Core hot loop — proven original implementation, untouched.
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
        if ($handle === false) throw new RuntimeException("Unable to open: {$inputPath}");

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
        if ($out === false) throw new RuntimeException("Unable to write: {$outputPath}");

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
