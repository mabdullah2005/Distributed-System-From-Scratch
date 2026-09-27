package com.kvstore.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class KVBenchmark {

    private final List<Integer> ports;
    private final int numThreads;
    private final int totalOps;
    private final int opsPerThread;
    private final int readPercentage;
    private final int warmups;

    private final ExecutorService executor;
    private final CountDownLatch startGun;
    private final CountDownLatch stopGun;
    private final List<List<Long>> allThreadLatencies;


    public KVBenchmark() {
        this(Arrays.asList(8081, 8082, 8083, 8084, 8085),
                8,
                5000,
                20,
                100);
    }


    public KVBenchmark(List<Integer> ports, int numThreads, int totalOps, int readPercentage, int warmups) {
        if (ports == null || ports.isEmpty()) {
            throw new IllegalArgumentException("Cluster ports cannot be null or empty.");
        }
        if (numThreads <= 0 || totalOps <= 0 || warmups < 0) {
            throw new IllegalArgumentException("Worker threads, total operations, and warmup ops must be positive.");
        }
        if (readPercentage < 0 || readPercentage > 100) {
            throw new IllegalArgumentException("Read percentage must be between 0 and 100 inclusive.");
        }

        this.ports = new ArrayList<>(ports);
        this.numThreads = numThreads;
        this.totalOps = totalOps;
        this.opsPerThread = totalOps / numThreads;
        this.readPercentage = readPercentage;
        this.warmups = warmups;

        this.executor = Executors.newFixedThreadPool(numThreads);
        this.startGun = new CountDownLatch(1);
        this.stopGun = new CountDownLatch(numThreads);
        this.allThreadLatencies = Collections.synchronizedList(new ArrayList<>());
    }

    public BenchmarkStats run() throws InterruptedException {
        try {
            if (warmups > 0) {
                System.out.printf("Warming up JVM and connection pools (%d ops)...%n", warmups);
                KVClient warmupClient = new KVClient(ports);
                for (int i = 0; i < warmups; i++) {
                    warmupClient.put("warmup_key_" + i, "warmup_val_" + i);
                }
                System.out.printf("Warmup complete. Launching benchmark with %d concurrent threads (%d ops total)...%n",
                        numThreads, totalOps);
            }

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    KVClient client = new KVClient(ports);
                    List<Long> threadLatencies = new ArrayList<>(opsPerThread);

                    try {
                        startGun.await();

                        for (int i = 0; i < opsPerThread; i++) {
                            boolean isRead = ThreadLocalRandom.current().nextInt(100) < readPercentage;
                            String key = "key_" + (i % 500);

                            long t0 = System.nanoTime();
                            if (isRead) {
                                client.get(key);
                            } else {
                                client.put(key, "value_" + threadId + "_" + i);
                            }
                            long t1 = System.nanoTime();
                            threadLatencies.add(t1 - t0);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        allThreadLatencies.add(threadLatencies);
                        stopGun.countDown();
                    }
                });
            }

            long startNs = System.nanoTime();
            startGun.countDown();

            stopGun.await();
            long totalDurationNs = System.nanoTime() - startNs;

            List<Long> mergedLatencies = new ArrayList<>(totalOps);
            for (List<Long> threadList : allThreadLatencies) {
                mergedLatencies.addAll(threadList);
            }

            BenchmarkStats stats = new BenchmarkStats(mergedLatencies, totalDurationNs);
            printReport(stats, totalDurationNs);
            return stats;

        } finally {
            executor.shutdown();
        }
    }


    private void printReport(BenchmarkStats stats, long totalDurationNs) {
        double durationSeconds = totalDurationNs / 1_000_000_000.0;
        System.out.println();
        System.out.println("==========================================================");
        System.out.println("             RAFT KV-STORE BENCHMARK REPORT               ");
        System.out.println("==========================================================");
        System.out.printf("  Concurrency:   %d worker threads%n", numThreads);
        System.out.printf("  Workload:      %d ops (%d%% Read, %d%% Write)%n", totalOps, readPercentage, 100 - readPercentage);
        System.out.printf("  Total Time:    %.3f seconds%n", durationSeconds);
        System.out.printf("  Throughput:    %.2f ops/sec%n", stats.getThroughput());
        System.out.println("----------------------------------------------------------");
        System.out.println("  Latency Distribution (Milliseconds):");
        System.out.printf("    Min:         %8.3f ms%n", stats.getMinLatencyMs());
        System.out.printf("    Mean:        %8.3f ms%n", stats.getMeanLatencyMs());
        System.out.printf("    P50 (Median):%8.3f ms%n", stats.getPercentileMs(50));
        System.out.printf("    P95:         %8.3f ms%n", stats.getPercentileMs(95));
        System.out.printf("    P99:         %8.3f ms%n", stats.getPercentileMs(99));
        System.out.printf("    Max:         %8.3f ms%n", stats.getMaxLatencyMs());
        System.out.println("==========================================================");
        System.out.println();
    }

    public static void main(String[] args) throws InterruptedException {
        KVBenchmark benchmark = new KVBenchmark();
        benchmark.run();
    }
}
