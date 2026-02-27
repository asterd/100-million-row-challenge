<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;
use RuntimeException;

final class Parser
{
    private const int READ_CHUNK    = 67_108_864; // 64 MB — meno syscall fread
    private const int DISCOVER_SIZE =  8_388_608; //  8 MB
    private const int URL_PREFIX_LEN = 25;        // strlen("https://stitcher.io/blog/")

    // Numero di worker determinato runtime in base ai core logici disponibili.
    // Capped a 16 per non saturare il fork overhead su macchine molto grandi.
    private int $workers;

    public function __construct()
    {
        $this->workers = $this->detectWorkers();
    }

    private function detectWorkers(): int
    {
        // Legge i core disponibili senza dipendenze esterne.
        $cores = 1;
        if (is_readable('/proc/cpuinfo')) {
            $cpuinfo = file_get_contents('/proc/cpuinfo');
            if ($cpuinfo !== false) {
                $cores = max(1, substr_count($cpuinfo, "\nprocessor\t:"));
            }
        } elseif (function_exists('shell_exec')) {
            $n = (int) shell_exec('nproc 2>/dev/null');
            if ($n > 0) {
                $cores = $n;
            }
        }
        // Usa tutti i core ma cap a 16
        return min(max(1, $cores), 16);
    }

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

        if (
            function_exists('pcntl_fork')
            && function_exists('pcntl_waitpid')
            && $fileSize > self::DISCOVER_SIZE
            && $this->workers > 1
        ) {
            $this->parseParallel(
                $inputPath,
                $outputPath,
                $fileSize,
                $pathIds,
                $paths,
                $pathCount,
                $dateIds,
                $dates,
                $dateCount,
            );

            return;
        }

        $counts = $this->parseRange(
            $inputPath,
            0,
            $fileSize,
            $pathIds,
            $dateIds,
            $pathCount,
            $dateCount,
        );
        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /** @return array{0: array<string,int>, 1: array<int,string>, 2:int} */
    private function buildDateMap(): array
    {
        $dateIds  = [];
        $dates    = [];
        $dateCount = 0;

        for ($y = 20; $y <= 26; $y++) {
            $yStr = ($y < 10 ? '0' : '') . $y;

            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2       => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };

                $mStr  = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';

                for ($d = 1; $d <= $maxD; $d++) {
                    $key             = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key]   = $dateCount;
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
        $discoverSize = $fileSize > self::DISCOVER_SIZE ? self::DISCOVER_SIZE : $fileSize;
        $chunk        = fread($handle, $discoverSize);
        fclose($handle);

        if (is_string($chunk) && $chunk !== '') {
            $lastNl    = strrpos($chunk, "\n");

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
     * @param array<string, int> $pathIds
     * @param array<int, string> $paths
     * @param array<string, int> $dateIds
     * @param array<int, string> $dates
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
    ): void {
        $workers    = $this->workers;
        $boundaries = [0];
        $bh         = fopen($inputPath, 'rb');

        if ($bh === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        for ($i = 1; $i < $workers; $i++) {
            fseek($bh, (int) ($fileSize * $i / $workers));
            fgets($bh);
            $boundaries[] = ftell($bh);
        }

        fclose($bh);
        $boundaries[] = $fileSize;

        $tmpDir  = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid   = getmypid();
        $children = [];

        // Forka tutti i worker tranne l'ultimo che gira nel processo padre.
        for ($w = 0; $w < $workers - 1; $w++) {
            $tmpFile = $tmpDir . '/p100m_' . $myPid . '_' . $w;
            $pid     = pcntl_fork();

            if ($pid === 0) {
                // --- WORKER CHILD ---
                $wCounts = $this->parseRange(
                    $inputPath,
                    $boundaries[$w],
                    $boundaries[$w + 1],
                    $pathIds,
                    $dateIds,
                    $pathCount,
                    $dateCount,
                );
                // Serializza con pack V* (uint32 little-endian, stesso del codice originale)
                $payload = pack('V*', ...$wCounts);
                if ($payload === '') {
                    exit(2);
                }
                $ok = file_put_contents($tmpFile, $payload);
                exit($ok === false ? 3 : 0);
            }

            if ($pid < 0) {
                throw new RuntimeException('Fork failed');
            }

            $children[] = [
                'pid'     => $pid,
                'start'   => $boundaries[$w],
                'end'     => $boundaries[$w + 1],
                'tmpFile' => $tmpFile,
            ];
        }

        // Padre elabora l'ultimo slice.
        $counts = $this->parseRange(
            $inputPath,
            $boundaries[$workers - 1],
            $boundaries[$workers],
            $pathIds,
            $dateIds,
            $pathCount,
            $dateCount,
        );

        foreach ($children as $child) {
            pcntl_waitpid($child['pid'], $status);
            $childOk = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;
            $payload = $childOk ? file_get_contents($child['tmpFile']) : false;
            @unlink($child['tmpFile']);

            if ($payload !== false && $payload !== '') {
                $wCounts = unpack('V*', $payload);
                if (is_array($wCounts)) {
                    $this->mergeCounts($counts, $wCounts);
                    continue;
                }
            }

            // Fallback se il worker ha fallito.
            $fallback = $this->parseRange(
                $inputPath,
                $child['start'],
                $child['end'],
                $pathIds,
                $dateIds,
                $pathCount,
                $dateCount,
            );
            $this->mergeCounts($counts, $fallback);
        }

        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    /** @param array<int, int> $counts @param iterable<int, int> $partial */
    private function mergeCounts(array &$counts, iterable $partial): void
    {
        $j = 0;
        foreach ($partial as $value) {
            $counts[$j++] += $value;
        }
    }

    /**
     * Loop critico — ottimizzato con explode() al posto di strpos() ripetuto
     * e carry buffer al posto di fseek() backward.
     *
     * @param array<string, int> $pathIds
     * @param array<string, int> $dateIds
     * @return array<int, int>
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
        // SplFixedArray evita la riallocazione della hash-map PHP per l'array piatto
        // e accede all'indice in O(1) senza overhead di bucket.
        $total  = $pathCount * $dateCount;
        $counts = new \SplFixedArray($total);
        for ($i = 0; $i < $total; $i++) {
            $counts[$i] = 0;
        }

        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;
        $carry     = '';           // buffer della riga parziale del chunk precedente
        $prefixLen = self::URL_PREFIX_LEN;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $raw    = fread($handle, $toRead);

            if ($raw === false || $raw === '') {
                break;
            }

            $chunkLen   = strlen($raw);
            $remaining -= $chunkLen;

            // Unisce il carry con il nuovo chunk.
            // Se il chunk non termina con \n, separa l'ultima riga parziale come nuovo carry.
            $chunk = $carry . $raw;
            $raw   = ''; // libera memoria subito

            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                // Non ci sono newline: tutto il chunk è carry (caso limite)
                $carry = $chunk;
                continue;
            }

            // Salva la parte dopo l'ultimo \n come carry per il prossimo round.
            $carry = substr($chunk, $lastNl + 1);

            // ----------------------------------------------------------------
            // HOT PATH: explode su \n produce un array di righe senza loop
            // strpos/strrpos ripetuti. Ogni elemento è già una riga completa.
            // ----------------------------------------------------------------
            $lines = explode("\n", substr($chunk, 0, $lastNl));
            $chunk = ''; // libera memoria

            foreach ($lines as $line) {
                if ($line === '') {
                    continue;
                }

                // Formato: https://stitcher.io/blog/<slug>,20YY-MM-DD HH:MM:SS,<ip>
                // commaPos = posizione della prima virgola dopo il prefisso
                $commaPos = strpos($line, ',', $prefixLen);

                if ($commaPos === false) {
                    continue; // riga malformata
                }

                // Slug: da $prefixLen a $commaPos
                $slug = substr($line, $prefixLen, $commaPos - $prefixLen);

                // Data: "20YY-MM-DD" parte da $commaPos+1, ma vogliamo solo "YY-MM-DD"
                // Il codice originale usa commaPos+3 per saltare "20" → 8 caratteri
                $dateKey = substr($line, $commaPos + 3, 8);

                // Accesso diretto senza isset() — fallisce silenziosamente
                // solo se slug/date non sono nel dizionario (non dovrebbe accadere).
                if (isset($pathIds[$slug], $dateIds[$dateKey])) {
                    $idx           = $pathIds[$slug] + $dateIds[$dateKey];
                    $counts[$idx]++;
                }
            }
        }

        fclose($handle);

        // Converte SplFixedArray in array PHP per compatibilità con mergeCounts
        return $counts->toArray();
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
            $base       = $p * $dateCount;
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
