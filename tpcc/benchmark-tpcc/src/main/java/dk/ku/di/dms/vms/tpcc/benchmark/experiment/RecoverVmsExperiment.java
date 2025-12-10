package dk.ku.di.dms.vms.tpcc.benchmark.experiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.tpcc.benchmark.Util.ComponentProcess;
import dk.ku.di.dms.vms.tpcc.benchmark.Util.Util;
import dk.ku.di.dms.vms.tpcc.benchmark.ingestion.*;
import dk.ku.di.dms.vms.tpcc.benchmark.workload.Workload;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils.loadCoordinator;

public class RecoverVmsExperiment
{
    private Coordinator coordinator;
    private final Map<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> CRASH_MAP = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> CRASH_ACK_MAP = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> RECONNECTION_MAP = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> RECONNECTION_ACK_MAP = new ConcurrentHashMap<>();
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

    public RecoverVmsExperiment(int numTransactions)
    {
        var numberOfAborts = 0;
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

        // keep a list
        coordinator.registerCrashConsumer((crashedVms) ->  {
            // System.out.println(STR."Registering CRASH in experiment consumer");
            CRASH_MAP.putIfAbsent(crashedVms, new ArrayList<>());
            CRASH_MAP.get(crashedVms).add(System.currentTimeMillis());
        });

        // keep a list
        coordinator.registerCrashAckConsumer((crashedVms) ->  {
            // System.out.println(STR."Registering CRASH ACK in experiment consumer");
            CRASH_ACK_MAP.putIfAbsent(crashedVms, new ArrayList<>());
            CRASH_ACK_MAP.get(crashedVms).add(System.currentTimeMillis());
        });

        coordinator.registerReconnectionConsumer((restartedVms) ->  {
            // System.out.println(STR."Registering RECONNECTION in experiment consumer");
            RECONNECTION_MAP.putIfAbsent(restartedVms, new ArrayList<>());
            RECONNECTION_MAP.get(restartedVms).add(System.currentTimeMillis());
        });

        coordinator.registerReconnectionAckConsumer((restartedVms) ->  {
            // System.out.println(STR."Registering RECONNECTION ACK in experiment consumer");
            RECONNECTION_ACK_MAP.putIfAbsent(restartedVms, new ArrayList<>());
            RECONNECTION_ACK_MAP.get(restartedVms).add(System.currentTimeMillis());
        });

        System.console().readLine();
        System.out.println(STR."ingesting data");
        this.ingestData();
    }

    public ExperimentResults runExperiment(int runTime, int warmup) throws IOException {
        int newRuntime = runTime + warmup;

        int crashPoint = warmup + 5000;
        int reconnectPoint = crashPoint + 5000;

        var globalInitTs = System.currentTimeMillis();

        Workload.submitNewOrders(newOrderWareInInput, coordinator, 1000, 25000);

        // coordinator has already sent the events ...

        Util.Sleep(warmup);
        for (int i = 0; i < 4; i++) {
            ComponentProcess.Kill("order");
            Util.Sleep(2000);
            ComponentProcess.StartVms("order", true, 1);
            Util.Sleep(3500);
        }

//        Util.Sleep(crashPoint);
//        ComponentProcess.Kill("order");
//
//        Util.Sleep(reconnectPoint-crashPoint);
//        ComponentProcess.StartVms("order", true, 1);
//        Util.Sleep(runTime-reconnectPoint);


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
            System.out.println(STR."batchStats endTs=\{batchStat.getValue().endTs()} !< initTs=\{initTs}");
            break;
        }

        // if none, consider the first batch as the warmup
        if(prevBatchStats == null) {
            Long lowestKey = BATCH_TO_FINISHED_TS_MAP.keySet().stream().min(Long::compareTo).orElse(null);
            prevBatchStats = BATCH_TO_FINISHED_TS_MAP.get(lowestKey);
        }

        BatchStats firstBatchStats = prevBatchStats;

        List<ThroughputInfo> throughputInfo = new ArrayList<>();
        List<CrashInfo> crashes = new ArrayList<>();
        List<ReconnectionInfo> reconnections = new ArrayList<>();

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

        for (var entry : CRASH_MAP.entrySet())
        {
            var crashedVms = entry.getKey();
            var timestampStarts = CRASH_MAP.get(crashedVms);
            var timestampEnds = CRASH_ACK_MAP.get(crashedVms);

            for (int i = 0; i < timestampEnds.size(); i++) {
                var timestampStart = timestampStarts.get(i);
                var timestampEnd = timestampEnds.get(i);

                System.out.println(STR."Crash: \{crashedVms} started \{timestampStart} and ended \{timestampEnd}");
                crashes.add(new CrashInfo(crashedVms, timestampStart, timestampEnd));
            }
        }

        for (var entry : RECONNECTION_MAP.entrySet())
        {
            var restartedVms = entry.getKey();
            var timestampStarts = RECONNECTION_MAP.get(restartedVms);
            var timestampEnds = RECONNECTION_ACK_MAP.get(restartedVms);

            for (int i = 0; i < timestampEnds.size(); i++) {
                var timestampStart = timestampStarts.get(i);
                var timestampEnd = timestampEnds.get(i);

                System.out.println(STR."Reconnection: \{restartedVms} started \{timestampStart} and ended \{timestampEnd}");
                reconnections.add(new ReconnectionInfo(restartedVms, timestampStart, timestampEnd));
            }
        }

        var results = ExperimentResults.Recovery(throughputInfo, crashes, reconnections);
        writeResultsToFile(results);

        return results;
    }


    // store it persistently in
    // default data
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
        String fileName = "result_vms_recovery.json";
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(fileName), results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
