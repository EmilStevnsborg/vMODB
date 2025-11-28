package dk.ku.di.dms.vms.tpcc.proxy.experiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentOrderIn;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

public final class ExperimentUtils {

    private static final System.Logger LOGGER = System.getLogger(ExperimentUtils.class.getName());

    private static boolean CONSUMER_REGISTERED = false;

    private static int lastExperimentNumCommitedTiDs = 0;

    public static ExperimentResults runExperiment(Coordinator coordinator, List<Iterator<NewOrderWareIn>> input, int runTime, int warmUp) {

        // provide a consumer to avoid depending on the coordinator
        Function<NewOrderWareIn, Long> func = newOrderInputBuilder(coordinator);

        if(CONSUMER_REGISTERED) {
            // clean up possible entries from previous run
            BATCH_TO_FINISHED_TS_MAP.clear();
        } else {
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

            CONSUMER_REGISTERED = true;
        }

        int newRuntime = runTime + warmUp;
        WorkloadUtils.WorkloadStats workloadStats = WorkloadUtils.submitWorkload(input, func);

        try {
            Thread.sleep(newRuntime);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // avoid submitting after experiment termination
        coordinator.clearTransactionInputs();
        LOGGER.log(INFO,"Transaction input queue(s) cleared.");

        if(BATCH_TO_FINISHED_TS_MAP.isEmpty()) {
            LOGGER.log(WARNING, "No batch of transactions completed!");
            return new ExperimentResults();
//            return new ExperimentStats(0, 0, 0, 0, 0, 0, 0, 0);
        }

        long endTs = workloadStats.initTs() + newRuntime;
        long initTs = workloadStats.initTs() + warmUp;
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

        return results;
    }

    public record ExperimentStats(long initTs, int numCompletedWithWarmUp, int numCompleted, double txPerSec, double average,
                                   double percentile_50, double percentile_75, double percentile_90){}

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
    public static void writeResultsToFile(int numWare, ExperimentStats expStats, int runTime, int warmUp,
                                          int numTransactionWorkers, int batchWindow, int maxTransactionsPerBatch){
        LocalDateTime time = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(expStats.initTs),
                ZoneId.systemDefault()
        );
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd_MM_yy_HH_mm_ss");
        String formattedDate = time.format(formatter);
        String fileName = "tpcc_" + formattedDate + ".txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write("======= TPC-C in vMODB =======");
            writer.newLine();
            writer.write("Experiment start: " + formattedDate);
            writer.newLine();
            writer.write("Experiment duration (ms): " + runTime);
            writer.newLine();
            writer.write("Experiment warm up (ms): " + warmUp);
            writer.newLine();
            writer.write("Batch window (ms): " + batchWindow);
            writer.newLine();
            writer.write("Max transactions per batch: " +maxTransactionsPerBatch);
            writer.newLine();
            writer.write("Number of transaction workers: " + numTransactionWorkers);
            writer.newLine();
            writer.write("Number of warehouses: " + numWare);
            writer.newLine();
            writer.newLine();
            writer.write("Average latency: "+ expStats.average);
            writer.newLine();
            writer.write("Latency at 50th percentile: "+ expStats.percentile_50);
            writer.newLine();
            writer.write("Latency at 75th percentile: "+ expStats.percentile_75);
            writer.newLine();
            writer.write("Latency at 90th percentile: "+ expStats.percentile_90);
            writer.newLine();
            writer.write("Number of completed transactions (with warm up): "+ expStats.numCompletedWithWarmUp);
            writer.newLine();
            writer.write("Number of completed transactions: "+ expStats.numCompleted);
            writer.newLine();
            writer.write("Throughput (tx/sec): "+expStats.txPerSec);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Map<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();

    private record BatchStats(long batchId, long numCommitedTiDs, long endTs){}

    private static final Map<Long, Long> ABORT_MAP = new ConcurrentHashMap<>();

    private static final Map<Long, Long> ABORT_ACK_MAP = new ConcurrentHashMap<>();

    private static Function<NewOrderWareIn, Long> newOrderInputBuilder(final Coordinator coordinator) {
        return newOrderWareIn -> {
            TransactionInput.Event eventPayload = new TransactionInput.Event("new-order-ware-in", newOrderWareIn.toString());
            TransactionInput txInput = new TransactionInput("new_order", eventPayload);
            coordinator.queueTransactionInput(txInput);
            return (long) BATCH_TO_FINISHED_TS_MAP.size() + 1;
        };
    }

    private static Function<PaymentOrderIn, Long>  paymentInputBuilder(final Coordinator coordinator) {
        return paymentOrderIn -> {
            TransactionInput.Event eventPayload = new TransactionInput.Event("payment-order-in", paymentOrderIn.toString());
            TransactionInput txInput = new TransactionInput("payment", eventPayload);
            coordinator.queueTransactionInput(txInput);
            return (long) BATCH_TO_FINISHED_TS_MAP.size() + 1;
        };
    }

    public static Coordinator loadCoordinator(Properties properties) {
        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG newOrderDag = TransactionBootstrap.name("new_order")
                .input("a", "warehouse", "new-order-ware-in")
                .internal("b", "inventory", "new-order-ware-out", "a")
                .terminal("c", "order", "b")
                .build();
        transactionMap.put(newOrderDag.name, newOrderDag);

        TransactionDAG paymentDag = TransactionBootstrap.name("payment")
                .input("a", "order", "payment-order-in")
                .terminal("b", "warehouse", "a")
                .build();
        transactionMap.put(paymentDag.name, paymentDag);

        Map<String, IdentifiableNode> starterVMSs = getVmsMap(properties);
        Coordinator coordinator = Coordinator.build(properties, starterVMSs, transactionMap, (ignored1) -> IHttpHandler.DEFAULT);
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();
        return coordinator;
    }

    private static Map<String, IdentifiableNode> getVmsMap(Properties properties) {
        String warehouseHost = properties.getProperty("warehouse_host");
        String inventoryHost = properties.getProperty("inventory_host");
        String orderHost = properties.getProperty("order_host");
        if(warehouseHost == null) throw new RuntimeException("Warehouse host is null");
        if(inventoryHost == null) throw new RuntimeException("Inventory host is null");
        if(orderHost == null) throw new RuntimeException("Order host is null");
        IdentifiableNode warehouseAddress = new IdentifiableNode("warehouse", warehouseHost, 8001);
        IdentifiableNode inventoryAddress = new IdentifiableNode("inventory", inventoryHost, 8002);
        IdentifiableNode orderAddress = new IdentifiableNode("order", orderHost, 8003);
        Map<String, IdentifiableNode> starterVMSs = new HashMap<>();
        starterVMSs.putIfAbsent(warehouseAddress.identifier, warehouseAddress);
        starterVMSs.putIfAbsent(inventoryAddress.identifier, inventoryAddress);
        starterVMSs.putIfAbsent(orderAddress.identifier, orderAddress);
        return starterVMSs;
    }

}
