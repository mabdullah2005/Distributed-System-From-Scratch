package com.kvstore.client;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


public class BenchmarkStatsTest {

    private static final double TOLERANCE = 0.001;
    private static final long NANOS_PER_MS = 1_000_000L;
    private static final long NANOS_PER_SEC = 1_000_000_000L;

    @Test
    void testKnownDistributionPercentiles() {
        // 100 samples from 1ms up to 100ms in nanoseconds
        List<Long> latencies = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            latencies.add(i * NANOS_PER_MS);
        }

        long durationNs = NANOS_PER_SEC;

        BenchmarkStats stats = new BenchmarkStats(latencies, durationNs);

        assertEquals(100.0, stats.getThroughput(), TOLERANCE);

        assertEquals(1.0, stats.getMinLatencyMs(), TOLERANCE);
        assertEquals(100.0, stats.getMaxLatencyMs(), TOLERANCE);

        assertEquals(50.5, stats.getMeanLatencyMs(), TOLERANCE);

        assertEquals(50.0, stats.getPercentileMs(50), TOLERANCE);
        assertEquals(95.0, stats.getPercentileMs(95), TOLERANCE);
        assertEquals(99.0, stats.getPercentileMs(99), TOLERANCE);
    }

    @Test
    void testSingleSample() {
        List<Long> latencies = List.of(5 * NANOS_PER_MS);
        long durationNs = 500 * NANOS_PER_MS; // 0.5s

        BenchmarkStats stats = new BenchmarkStats(latencies, durationNs);

        assertEquals(2.0, stats.getThroughput(), TOLERANCE);
        assertEquals(5.0, stats.getMinLatencyMs(), TOLERANCE);
        assertEquals(5.0, stats.getMaxLatencyMs(), TOLERANCE);
        assertEquals(5.0, stats.getMeanLatencyMs(), TOLERANCE);
        assertEquals(5.0, stats.getPercentileMs(50), TOLERANCE);
        assertEquals(5.0, stats.getPercentileMs(95), TOLERANCE);
        assertEquals(5.0, stats.getPercentileMs(99), TOLERANCE);
    }

    @Test
    void testUnsortedLatenciesWithoutSideEffects() {
        List<Long> latencies = new ArrayList<>(List.of(
                50 * NANOS_PER_MS,
                10 * NANOS_PER_MS,
                30 * NANOS_PER_MS,
                20 * NANOS_PER_MS,
                40 * NANOS_PER_MS
        ));
        List<Long> originalCopy = List.copyOf(latencies);

        BenchmarkStats stats = new BenchmarkStats(latencies, NANOS_PER_SEC);

        assertEquals(10.0, stats.getMinLatencyMs(), TOLERANCE);
        assertEquals(50.0, stats.getMaxLatencyMs(), TOLERANCE);
        assertEquals(30.0, stats.getPercentileMs(50), TOLERANCE);

        assertEquals(originalCopy, latencies, "Constructor must not mutate the caller's input list in place");
    }

    @Test
    void testDefensiveValidation() {
        assertThrows(IllegalArgumentException.class, () -> new BenchmarkStats(null, NANOS_PER_SEC));
        assertThrows(IllegalArgumentException.class, () -> new BenchmarkStats(Collections.emptyList(), NANOS_PER_SEC));

        assertThrows(IllegalArgumentException.class, () -> new BenchmarkStats(List.of(10L), 0));
        assertThrows(IllegalArgumentException.class, () -> new BenchmarkStats(List.of(10L), -100));
    }
}
