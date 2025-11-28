package dk.ku.di.dms.vms.flightScheduler.benchmark.experiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.flightScheduler.benchmark.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.common.events.OrderFlight;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.modb.utils.StorageUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.ORDER_FLIGHT;
import static java.lang.System.Logger.Level.*;

public class Experiment
{
    private static int lastExperimentNumCommitedTiDs = 0;
    private static final Map<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();

    private record BatchStats(long batchId, long numCommitedTiDs, long endTs){}

    private static final Map<Long, Long> ABORT_MAP = new ConcurrentHashMap<>();

    private static final Map<Long, Long> ABORT_ACK_MAP = new ConcurrentHashMap<>();

    private static final Random generate = new Random();
    public static int randomNumber(int min, int max) {
        int next = generate.nextInt();
        int div = next % ((max - min) + 1);
        if (div < 0) {
            div = div * -1;
        }
        return min + div;
    }
    public static ExperimentResults runAbortExperiment(Coordinator coordinator, int runTime, int warmup, Iterator<OrderFlight> input)
    {
        coordinator.registerBatchCommitConsumer((batchId, commitedTiDs) -> {
            BATCH_TO_FINISHED_TS_MAP.put(
                    batchId,
                    new BatchStats(batchId, commitedTiDs, System.currentTimeMillis()));
        });

        coordinator.registerAbortConsumer((abortedTid) ->  {
            ABORT_MAP.put(abortedTid, System.currentTimeMillis());
        });

        coordinator.registerAbortAckConsumer((abortedTid) ->  {
            ABORT_ACK_MAP.put(abortedTid, System.currentTimeMillis());
        });

        int newRuntime = runTime + warmup;
        WorkloadStats workloadStats = submitOrderFlights(input, newOrderFlightBuilder(coordinator));

        Util.Sleep(newRuntime);

        // avoid submitting after experiment termination
        coordinator.clearTransactionInputs();

        if(BATCH_TO_FINISHED_TS_MAP.isEmpty()) {
            return new ExperimentResults();
        }

        long endTs = workloadStats.initTs() + newRuntime;
        long initTs = workloadStats.initTs() + warmup;
        int numCompletedWithWarmUp;
        int numCompletedDuringWarmUp = 0;
        int numCompleted;
        List<Long> allLatencies = new ArrayList<>();

        // find first batch that runs transactions after warm up
        BatchStats prevBatchStats = null;
        for(var batchStat : BATCH_TO_FINISHED_TS_MAP.entrySet()){
            if(batchStat.getValue().endTs < initTs) {
                prevBatchStats = batchStat.getValue();
                numCompletedDuringWarmUp = (int) prevBatchStats.numCommitedTiDs - lastExperimentNumCommitedTiDs;
                continue;
            }
            System.out.println(STR."batchStas endTs=\{batchStat.getValue().endTs} !< initTs=\{initTs}");
            break;
        }

        // if none, consider the first batch as the warmup
        if(prevBatchStats == null) {
            Long lowestKey = BATCH_TO_FINISHED_TS_MAP.keySet().stream().min(Long::compareTo).orElse(null);
            prevBatchStats = BATCH_TO_FINISHED_TS_MAP.get(lowestKey);
            numCompletedDuringWarmUp = (int) prevBatchStats.numCommitedTiDs - lastExperimentNumCommitedTiDs;
        }

        BatchStats firstBatchStats = prevBatchStats;

        List<ThroughputInfo> throughputInfo = new ArrayList<>();
        List<AbortInfo> aborts = new ArrayList<>();

        // calculate latency based on the batch
        // calculate committed transactions in each time span
        while(BATCH_TO_FINISHED_TS_MAP.containsKey(prevBatchStats.batchId+1)){
            BatchStats currBatchStats = BATCH_TO_FINISHED_TS_MAP.get(prevBatchStats.batchId+1);
            if(currBatchStats.endTs > endTs) break;

            var transactionsInTimeSpan = currBatchStats.numCommitedTiDs - prevBatchStats.numCommitedTiDs;
            var info = new ThroughputInfo(prevBatchStats.endTs, currBatchStats.endTs, transactionsInTimeSpan);
            throughputInfo.add(info);

            allLatencies.add(currBatchStats.endTs - prevBatchStats.endTs);
            prevBatchStats = currBatchStats;
        }

        for (var entry : ABORT_MAP.entrySet())
        {
            var abortedTid = entry.getKey();
            var timestampStart = entry.getValue();
            var timestampEnd = ABORT_ACK_MAP.get(abortedTid);

            if (timestampStart == null) timestampStart = 0L;
            if (timestampEnd == null) timestampEnd = 0L;

            aborts.add(new AbortInfo(abortedTid, timestampStart, timestampEnd));
        }

        var results = new ExperimentResults(throughputInfo, aborts);
        writeResultsToFile(results);

        return results;
    }

    public record WorkloadStats(long initTs){}

    // ONLY FOR WORKLOAD SUBMISSION
    private static Function<OrderFlight, Long> newOrderFlightBuilder(final Coordinator coordinator) {
        return orderFlight -> {
            var eventPayload = new TransactionInput.Event(ORDER_FLIGHT, orderFlight.toString());
            var txInput = new TransactionInput(ORDER_FLIGHT, eventPayload);
            coordinator.queueTransactionInput(txInput);
            return (long) BATCH_TO_FINISHED_TS_MAP.size() + 1;
        };
    }

    private static WorkloadStats submitOrderFlights(Iterator<OrderFlight> orderFlightsIterator, Function<OrderFlight, Long> orderFlightFunction)
    {
        Thread thread = new Thread(() -> {
            while (orderFlightsIterator.hasNext()) {
                try {
                    orderFlightFunction.apply(orderFlightsIterator.next());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();
        return new WorkloadStats(System.currentTimeMillis());
    }

    public static void writeResultsToFile(ExperimentResults results)
    {
        String fileName = "tpcc_result.json";
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(fileName), results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
