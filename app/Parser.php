<?php

namespace App;

use RuntimeException;

final class Parser
{
    private const HOST_PREFIX = 'https://stitcher.io';
    private const HOST_PREFIX_LENGTH = 19;
    private const READ_CHUNK_BYTES = 67_108_864;
    private const MIN_SIZE_FOR_PARALLEL = 67_108_864; // 64MB
    private const WORKERS = 2;
    private const SHM_BYTES = 536_870_912; // 512MB

    public function parse(string $inputPath, string $outputPath): void
    {
        if (function_exists('gc_disable')) {
            gc_disable();
        }

        $counts = $this->parseWithBestStrategy($inputPath);

        foreach ($counts as &$days) {
            ksort($days, SORT_NUMERIC);
        }
        unset($days);

        $this->writeOutputJsonEncode($counts, $outputPath);
    }

    /** @return array<string, array<int, int>> */
    private function parseWithBestStrategy(string $inputPath): array
    {
        $fileSize = filesize($inputPath);

        if ($fileSize === false) {
            throw new RuntimeException("Unable to read input file size: {$inputPath}");
        }

        if ($fileSize === 0) {
            return [];
        }

        if (! function_exists('pcntl_fork') || $fileSize < self::MIN_SIZE_FOR_PARALLEL) {
            return $this->parseRange($inputPath, 0, $fileSize);
        }

        return $this->parseParallel($inputPath, self::WORKERS, $fileSize);
    }

    /** @return array<string, array<int, int>> */
    private function parseParallel(string $inputPath, int $workers, int $fileSize): array
    {
        if (! function_exists('shm_attach') || ! function_exists('shm_put_var')) {
            return $this->parseParallelFile($inputPath, $workers, $fileSize);
        }

        $ranges = $this->calculateRanges($inputPath, $workers, $fileSize);

        if (count($ranges) <= 1) {
            return $this->parseRange($inputPath, 0, $fileSize);
        }

        $shmKey = random_int(0x10000, 0x7fffffff);
        $shm = @shm_attach($shmKey, self::SHM_BYTES, 0600);

        if ($shm === false) {
            return $this->parseParallelFile($inputPath, $workers, $fileSize);
        }

        $children = [];
        $merged = [];

        try {
            foreach ($ranges as $index => [$start, $end]) {
                $pid = pcntl_fork();

                if ($pid === -1) {
                    throw new RuntimeException('Unable to fork worker process');
                }

                if ($pid === 0) {
                    $result = $this->parseRange($inputPath, $start, $end);
                    $ok = @shm_put_var($shm, $index + 1, $result);

                    if (! $ok) {
                        exit(1);
                    }

                    exit(0);
                }

                $children[$pid] = $index;
            }

            while ($children !== []) {
                $pid = pcntl_wait($status);

                if ($pid <= 0) {
                    break;
                }

                $index = $children[$pid] ?? null;
                unset($children[$pid]);

                if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                    throw new RuntimeException("Worker process failed for chunk {$index}");
                }
            }

            if ($children !== []) {
                throw new RuntimeException('One or more worker processes did not finish correctly');
            }

            for ($i = 0, $total = count($ranges); $i < $total; $i++) {
                $varKey = $i + 1;

                if (! @shm_has_var($shm, $varKey)) {
                    throw new RuntimeException("Missing shared-memory payload for chunk {$i}");
                }

                $partial = @shm_get_var($shm, $varKey);

                if (! is_array($partial)) {
                    throw new RuntimeException("Invalid shared-memory payload for chunk {$i}");
                }

                foreach ($partial as $path => $days) {
                    if (! isset($merged[$path])) {
                        $merged[$path] = $days;
                        continue;
                    }

                    foreach ($days as $dayKey => $count) {
                        if (isset($merged[$path][$dayKey])) {
                            $merged[$path][$dayKey] += $count;
                        } else {
                            $merged[$path][$dayKey] = $count;
                        }
                    }
                }
            }
        } finally {
            @shm_remove($shm);
            @shm_detach($shm);
        }

        return $merged;
    }

    /** @return array<string, array<int, int>> */
    private function parseParallelFile(string $inputPath, int $workers, int $fileSize): array
    {
        $ranges = $this->calculateRanges($inputPath, $workers, $fileSize);

        if (count($ranges) <= 1) {
            return $this->parseRange($inputPath, 0, $fileSize);
        }

        $tempDir = sprintf('%s/parser-file-%d-%s', sys_get_temp_dir(), getmypid(), bin2hex(random_bytes(6)));

        if (! mkdir($tempDir, 0700) && ! is_dir($tempDir)) {
            throw new RuntimeException("Unable to create temp directory: {$tempDir}");
        }

        $children = [];
        $merged = [];
        $useIgbinary = function_exists('igbinary_serialize') && function_exists('igbinary_unserialize');

        try {
            foreach ($ranges as $index => [$start, $end]) {
                $pid = pcntl_fork();

                if ($pid === -1) {
                    throw new RuntimeException('Unable to fork worker process');
                }

                if ($pid === 0) {
                    $partialPath = "{$tempDir}/part-{$index}.bin";
                    $result = $this->parseRange($inputPath, $start, $end);
                    $encoded = $useIgbinary ? igbinary_serialize($result) : serialize($result);

                    if (! is_string($encoded) || file_put_contents($partialPath, $encoded) === false) {
                        exit(1);
                    }

                    exit(0);
                }

                $children[$pid] = $index;
            }

            while ($children !== []) {
                $pid = pcntl_wait($status);

                if ($pid <= 0) {
                    break;
                }

                $index = $children[$pid] ?? null;
                unset($children[$pid]);

                if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                    throw new RuntimeException("Worker process failed for chunk {$index}");
                }
            }

            if ($children !== []) {
                throw new RuntimeException('One or more worker processes did not finish correctly');
            }

            for ($i = 0, $total = count($ranges); $i < $total; $i++) {
                $partialPath = "{$tempDir}/part-{$i}.bin";
                $payload = file_get_contents($partialPath);

                if ($payload === false) {
                    throw new RuntimeException("Unable to read partial result: {$partialPath}");
                }

                $partial = $useIgbinary
                    ? igbinary_unserialize($payload)
                    : unserialize($payload, ['allowed_classes' => false]);

                if (! is_array($partial)) {
                    throw new RuntimeException("Invalid partial payload: {$partialPath}");
                }

                foreach ($partial as $path => $days) {
                    if (! isset($merged[$path])) {
                        $merged[$path] = $days;
                        continue;
                    }

                    foreach ($days as $dayKey => $count) {
                        if (isset($merged[$path][$dayKey])) {
                            $merged[$path][$dayKey] += $count;
                        } else {
                            $merged[$path][$dayKey] = $count;
                        }
                    }
                }
            }
        } finally {
            $files = glob("{$tempDir}/part-*.bin");

            if (is_array($files)) {
                foreach ($files as $file) {
                    @unlink($file);
                }
            }

            @rmdir($tempDir);
        }

        return $merged;
    }

    /** @return list<array{0:int,1:int}> */
    private function calculateRanges(string $inputPath, int $workers, int $fileSize): array
    {
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        $ranges = [];
        $start = 0;

        for ($i = 1; $i < $workers; $i++) {
            $target = (int) floor(($fileSize * $i) / $workers);

            if ($target <= $start) {
                continue;
            }

            if (fseek($handle, $target) !== 0) {
                break;
            }

            fgets($handle);
            $end = ftell($handle);

            if ($end === false || $end <= $start || $end >= $fileSize) {
                continue;
            }

            $ranges[] = [$start, $end];
            $start = $end;
        }

        fclose($handle);

        if ($start < $fileSize) {
            $ranges[] = [$start, $fileSize];
        }

        return $ranges;
    }

    /** @return array<string, array<int, int>> */
    private function parseRange(string $inputPath, int $start, int $end): array
    {
        if ($end <= $start) {
            return [];
        }

        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        if (fseek($handle, $start) !== 0) {
            fclose($handle);
            throw new RuntimeException("Unable to seek input file: {$inputPath}");
        }

        stream_set_read_buffer($handle, self::READ_CHUNK_BYTES);

        $paths = [];
        $uriToPid = [];
        $pathToPid = [];
        $countsByPid = [];

        $remaining = $end - $start;
        $carry = '';

        while ($remaining > 0) {
            $toRead = min(self::READ_CHUNK_BYTES, $remaining);
            $chunk = fread($handle, $toRead);

            if ($chunk === false || $chunk === '') {
                break;
            }

            $remaining -= strlen($chunk);
            $carry = $this->consumeBuffer($carry . $chunk, $paths, $uriToPid, $pathToPid, $countsByPid);
        }

        fclose($handle);

        if ($carry !== '') {
            $this->consumeBuffer($carry . "\n", $paths, $uriToPid, $pathToPid, $countsByPid);
        }

        $counts = [];

        foreach ($countsByPid as $pid => $days) {
            $counts[$paths[$pid]] = $days;
        }

        return $counts;
    }

    /**
     * @param array<int, string> $paths
     * @param array<string, int> $uriToPid
     * @param array<string, int> $pathToPid
     * @param array<int, array<int, int>> $countsByPid
     */
    private function consumeBuffer(string $buffer, array &$paths, array &$uriToPid, array &$pathToPid, array &$countsByPid): string
    {
        $offset = 0;
        $length = strlen($buffer);

        while (true) {
            $newline = strpos($buffer, "\n", $offset);

            if ($newline === false) {
                break;
            }

            $lineStart = $offset;
            $lineEnd = $newline;
            $offset = $newline + 1;

            if ($lineEnd <= $lineStart) {
                continue;
            }

            if ($buffer[$lineEnd - 1] === "\r") {
                $lineEnd--;
            }

            $comma = strpos($buffer, ',', $lineStart);

            if ($comma === false || $comma >= $lineEnd) {
                continue;
            }

            $uri = substr($buffer, $lineStart, $comma - $lineStart);

            if (isset($uriToPid[$uri])) {
                $pid = $uriToPid[$uri];
            } else {
                $pathStart = $lineStart + self::HOST_PREFIX_LENGTH;

                if (
                    $comma > $pathStart
                    && ($buffer[$pathStart] ?? '') === '/'
                    && substr_compare($buffer, self::HOST_PREFIX, $lineStart, self::HOST_PREFIX_LENGTH) === 0
                ) {
                    $path = substr($buffer, $pathStart, $comma - $pathStart);
                } else {
                    $path = parse_url($uri, PHP_URL_PATH);

                    if (! is_string($path) || $path === '') {
                        continue;
                    }
                }

                if (isset($pathToPid[$path])) {
                    $pid = $pathToPid[$path];
                } else {
                    $pid = count($paths);
                    $paths[$pid] = $path;
                    $pathToPid[$path] = $pid;
                }

                $uriToPid[$uri] = $pid;
            }

            $d = $comma + 1;

            if (($d + 9) >= $lineEnd) {
                continue;
            }

            $dayKey = (
                (
                    ((ord($buffer[$d]) - 48) * 1000)
                    + ((ord($buffer[$d + 1]) - 48) * 100)
                    + ((ord($buffer[$d + 2]) - 48) * 10)
                    + (ord($buffer[$d + 3]) - 48)
                ) * 10000
            )
                + (((ord($buffer[$d + 5]) - 48) * 10) + (ord($buffer[$d + 6]) - 48)) * 100
                + ((ord($buffer[$d + 8]) - 48) * 10) + (ord($buffer[$d + 9]) - 48);

            if (isset($countsByPid[$pid][$dayKey])) {
                $countsByPid[$pid][$dayKey]++;
            } else {
                $countsByPid[$pid][$dayKey] = 1;
            }
        }

        return $offset < $length ? substr($buffer, $offset) : '';
    }

    /** @param array<string, array<int, int>> $counts */
    private function writeOutputJsonEncode(array $counts, string $outputPath): void
    {
        $dayCache = [];
        $out = [];

        foreach ($counts as $path => $days) {
            $formatted = [];

            foreach ($days as $dayKey => $count) {
                $day = (int) $dayKey;
                $formatted[$dayCache[$day] ??= $this->formatDayKey($day)] = $count;
            }

            $out[$path] = $formatted;
        }

        $json = json_encode($out, JSON_PRETTY_PRINT);

        if ($json === false) {
            throw new RuntimeException('Unable to encode output JSON');
        }

        if (file_put_contents($outputPath, $json) === false) {
            throw new RuntimeException("Unable to write output file: {$outputPath}");
        }
    }

    private function formatDayKey(int $dayKey): string
    {
        $year = intdiv($dayKey, 10000);
        $monthDay = $dayKey % 10000;
        $month = intdiv($monthDay, 100);
        $day = $monthDay % 100;

        return $year
            . '-'
            . ($month < 10 ? '0' : '') . $month
            . '-'
            . ($day < 10 ? '0' : '') . $day;
    }
}
