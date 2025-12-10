package dk.ku.di.dms.vms.tpcc.benchmark.experiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.tpcc.benchmark.Util.ComponentProcess;
import dk.ku.di.dms.vms.tpcc.benchmark.Util.Util;
import dk.ku.di.dms.vms.tpcc.benchmark.ingestion.*;
import dk.ku.di.dms.vms.tpcc.benchmark.workload.Workload;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Stock;

import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;
import java.util.*;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils.loadCoordinator;

public class AbortExperiment
{
    private boolean initialized;
    private Coordinator coordinator;
    private final Map<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();
    private final Map<Long, Long> ABORT_MAP = new ConcurrentHashMap<>();
    private final Map<Long, Long> ABORT_ACK_MAP = new ConcurrentHashMap<>();
    private final Random generate = new Random();
    private final Iterator<NewOrderWareIn> newOrderWareInInput;

    public int randomNumber(int min, int max) {
        int next = generate.nextInt();
        int div = next % ((max - min) + 1);
        if (div < 0) {
            div = div * -1;
        }
        return min + div;
    }

    public AbortExperiment(int numTransactions)
    {
        // half of the transactions are order flights starting at the midway point

//        var numberOfAborts = 1;
        var numberOfAborts = numTransactions/25000;
        this.newOrderWareInInput = Workload.createNewOrderIterator(numTransactions, numberOfAborts);
    }

    public void initExperiment(Properties coordinatorProperties) throws Exception
    {
        ComponentProcess.KillVMSes();
        System.out.println(STR."killed VMSes");

        System.console().readLine();
        System.out.println(STR."starting VMSes");
        ComponentProcess.StartVMSes();

        System.console().readLine();
        System.out.println(STR."starting coordinator");
        Coordinator coordinator = loadCoordinator(coordinatorProperties);
        this.coordinator = coordinator;

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

        System.console().readLine();
        System.out.println(STR."ingesting data");
        this.ingestData();
    }

    public ExperimentResults runExperiment(int runTime, int warmup)
    {
        int newRuntime = runTime + warmup;
        var globalInitTs = System.currentTimeMillis();
        Workload.submitNewOrders(newOrderWareInInput, coordinator, 1000, 25000);

        Util.Sleep(newRuntime);

        // avoid submitting after experiment termination
        coordinator.clearTransactionInputs();

        if(BATCH_TO_FINISHED_TS_MAP.isEmpty()) {
            return new ExperimentResults();
        }

        long endTs = globalInitTs + newRuntime;
        long initTs = globalInitTs + warmup;
        List<Long> allLatencies = new ArrayList<>();

        // find first batch that runs transactions after warm up
        BatchStats prevBatchStats = null;
        for(var batchStat : BATCH_TO_FINISHED_TS_MAP.entrySet()){
            if(batchStat.getValue().endTs() < initTs) {
                prevBatchStats = batchStat.getValue();
                continue;
            }
            System.out.println(STR."batchStas endTs=\{batchStat.getValue().endTs()} !< initTs=\{initTs}");
            break;
        }

        // if none, consider the first batch as the warmup
        if(prevBatchStats == null) {
            Long lowestKey = BATCH_TO_FINISHED_TS_MAP.keySet().stream().min(Long::compareTo).orElse(null);
            prevBatchStats = BATCH_TO_FINISHED_TS_MAP.get(lowestKey);
        }

        BatchStats firstBatchStats = prevBatchStats;

        List<ThroughputInfo> throughputInfo = new ArrayList<>();
        List<AbortInfo> aborts = new ArrayList<>();

        // calculate latency based on the batch
        // calculate committed transactions in each time span
        while(BATCH_TO_FINISHED_TS_MAP.containsKey(prevBatchStats.batchId()+1)){
            BatchStats currBatchStats = BATCH_TO_FINISHED_TS_MAP.get(prevBatchStats.batchId()+1);
            if(currBatchStats.endTs() > endTs) break;

            var transactionsInTimeSpan = currBatchStats.numCommitedTiDs() - prevBatchStats.numCommitedTiDs();
            var info = new ThroughputInfo(prevBatchStats.endTs(), currBatchStats.endTs(), transactionsInTimeSpan);
            throughputInfo.add(info);

            allLatencies.add(currBatchStats.endTs() - prevBatchStats.endTs());
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

        var results = ExperimentResults.Abort(throughputInfo, aborts);
        writeResultsToFile(results);

        return results;
    }


    private void ingestData()
    {
        // flight, customer, booking
//        var totalTasks = 2*numIngestionWorkers + numIngestionWorkers/2;
        var totalTasks = 5;

        ExecutorService threadPool = Executors.newFixedThreadPool(totalTasks);
        BlockingQueue<Future<Void>> completionQueue = new ArrayBlockingQueue<>(totalTasks);
        CompletionService<Void> service = new ExecutorCompletionService<>(threadPool, completionQueue);

        long init = System.currentTimeMillis();
        long end;

        service.submit(new IngestionWorkerStock(), null);
        service.submit(new IngestionWorkerWarehouse(), null);
        service.submit(new IngestionWorkerCustomer(), null);
        service.submit(new IngestionWorkerDistrict(), null);
        service.submit(new IngestionWorkerItem(), null);

        try {
            for (int i = 0; i < totalTasks; i++) {
                Future<Void> f = service.take();  // waits for completion
                try {
                    f.get(); // propagate exceptions
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch(InterruptedException e){
            threadPool.shutdownNow();
            e.printStackTrace(System.err);
        } finally{
            end = System.currentTimeMillis();
        }
        System.out.println(STR."ingesting data took time: \{end-init}ms");

        // commit ingested data
        var client = HttpClient.newHttpClient();
        // Commit(client);
    }


    public static void writeResultsToFile(ExperimentResults results)
    {
        String fileName = "result_abort.json";
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(fileName), results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
