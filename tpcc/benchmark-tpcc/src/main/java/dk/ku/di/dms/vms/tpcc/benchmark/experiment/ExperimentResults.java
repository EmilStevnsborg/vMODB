package dk.ku.di.dms.vms.tpcc.benchmark.experiment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExperimentResults
{
    public List<ThroughputInfo> throughputInfo;
    public List<AbortInfo> aborts;
    public List<CrashInfo> crashes;
    public List<ReconnectionInfo> reconnections;

    public ExperimentResults() {
        throughputInfo = new ArrayList<>();
        aborts = new ArrayList<>();
        crashes = new ArrayList<>();
        reconnections = new ArrayList<>();
    }
    public ExperimentResults(List<ThroughputInfo> throughputInfo, List<AbortInfo> aborts, List<CrashInfo> crashes, List<ReconnectionInfo> reconnections) {
        this.throughputInfo = throughputInfo;
        this.aborts = aborts;
        this.crashes = crashes;
        this.reconnections = reconnections;

        Summary();
    }
    public static ExperimentResults Baseline(List<ThroughputInfo> throughputInfo) {
        return new ExperimentResults(throughputInfo, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public static ExperimentResults Abort(List<ThroughputInfo> throughputInfo, List<AbortInfo> aborts) {
        return new ExperimentResults(throughputInfo, aborts, new ArrayList<>(), new ArrayList<>());
    }
    public static ExperimentResults Recovery(List<ThroughputInfo> throughputInfo, List<CrashInfo> crashes, List<ReconnectionInfo> reconnections) {
        return new ExperimentResults(throughputInfo, new ArrayList<>(), crashes, reconnections);
    }
    private void Summary()
    {

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
