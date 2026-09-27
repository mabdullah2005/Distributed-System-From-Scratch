package com.kvstore.client;

import java.util.*;

public class BenchmarkStats {
    private final List<Long> latencies;
    private final long duration;

    public static final double TO_MS = 1000000.0;
    public static final double NANOS_PER_SEC = 1_000_000_000.0;

    public BenchmarkStats(List<Long> latencies, long duration){
        if(latencies == null || latencies.isEmpty() || duration <= 0){
            throw new IllegalArgumentException("latencies list is null and/or duration is less than or equal to 0.");
        }

        this.latencies = new ArrayList<>(latencies);
        Collections.sort(this.latencies);
        this.duration = duration;
    }

    public double getThroughput(){
        return latencies.size()/(duration / NANOS_PER_SEC);
    }

    public double getMinLatencyMs(){
        return latencies.get(0)/TO_MS;
    }

    public double getMaxLatencyMs(){
        return latencies.get(latencies.size() - 1)/TO_MS;
    }

    public double getMeanLatencyMs(){
        long total = 0;
        for(long latency: latencies){
            total += latency;
        }

        return ((double) total/latencies.size())/TO_MS;
    }

    public double getPercentileMs(double percentile){
        int index = (int) Math.ceil((percentile / 100.0) * this.latencies.size()) - 1;
        return latencies.get(index)/TO_MS;
    }
}
