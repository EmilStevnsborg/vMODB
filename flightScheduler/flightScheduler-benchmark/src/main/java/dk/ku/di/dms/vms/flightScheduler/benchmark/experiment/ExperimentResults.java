package dk.ku.di.dms.vms.flightScheduler.benchmark.experiment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExperimentResults
{
    public List<ThroughputInfo> throughputInfo;
    public List<AbortInfo> aborts;

    public ExperimentResults() {
        throughputInfo = new ArrayList<>();
        aborts = new ArrayList<>();
    }
    public ExperimentResults(List<ThroughputInfo> throughputInfo, List<AbortInfo> aborts) {
        this.throughputInfo = throughputInfo;
        this.aborts = aborts;

        var numCommittedTiDsTotal = throughputInfo.stream().mapToLong(info -> info.numCommittedTiDs()).sum();
        var globalTimestampStart = throughputInfo.stream().mapToLong(info -> info.timestampStart()).min().getAsLong();
        var globalTimestampEnd = throughputInfo.stream().mapToLong(info -> info.timestampEnd()).max().getAsLong();
        long actualRuntime = globalTimestampEnd - globalTimestampStart;

        var allLatencies = throughputInfo.stream()
                .map(info -> info.timestampEnd()-info.timestampStart())
                .collect(Collectors.toCollection(ArrayList::new));

        double average = allLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        allLatencies.sort(null);
        double percentile_50 = PercentileCalculator.calculatePercentile(allLatencies, 0.50);
        double percentile_75 = PercentileCalculator.calculatePercentile(allLatencies, 0.75);
        double percentile_90 = PercentileCalculator.calculatePercentile(allLatencies, 0.90);
        double txPerSec = numCommittedTiDsTotal / ((double) actualRuntime / 1000L);

        System.out.println("Average latency: "+ average);
        System.out.println("Latency at 50th percentile: "+ percentile_50);
        System.out.println("Latency at 75th percentile: "+ percentile_75);
        System.out.println("Latency at 90th percentile: "+ percentile_90);
        System.out.println("Number of completed transactions (during warm up): "+ numCommittedTiDsTotal);
        System.out.println("Number of completed transactions (after warm up): "+ numCommittedTiDsTotal);
        System.out.println("Number of completed transactions (total): "+ numCommittedTiDsTotal);
        System.out.println("Transactions per second: "+txPerSec);
        System.out.println();
    }
}
