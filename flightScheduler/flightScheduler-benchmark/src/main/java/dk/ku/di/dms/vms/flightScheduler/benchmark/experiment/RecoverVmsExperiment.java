package dk.ku.di.dms.vms.flightScheduler.benchmark.experiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.flightScheduler.benchmark.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.benchmark.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion.IngestionWorkerBooking;
import dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion.IngestionWorkerCustomer;
import dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion.IngestionWorkerFlight;
import dk.ku.di.dms.vms.flightScheduler.benchmark.workload.Workload;
import dk.ku.di.dms.vms.flightScheduler.common.events.OrderFlight;
import dk.ku.di.dms.vms.flightScheduler.common.events.PayBooking;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.flightScheduler.proxy.Main.loadCoordinator;

public class RecoverVmsExperiment
{
    private Coordinator coordinator;
    private final Map<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> CRASH_MAP = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> CRASH_ACK_MAP = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> RECONNECTION_MAP = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> RECONNECTION_ACK_MAP = new ConcurrentHashMap<>();
    private final Random generate = new Random();
    private final int numRecords;
    private final int numIngestionWorkers;
    private final Iterator<OrderFlight> orderFlightInput;
    private final Iterator<PayBooking> payBookingInput;

    public int randomNumber(int min, int max) {
        int next = generate.nextInt();
        int div = next % ((max - min) + 1);
        if (div < 0) {
            div = div * -1;
        }
        return min + div;
    }

    public RecoverVmsExperiment(int numRecords, int numTransactions, int numIngestionWorkers)
    {
        this.numRecords = numRecords;
        this.numIngestionWorkers = numIngestionWorkers;

        // half of the transactions are order flights starting at the midway point
        var numberOfAborts = 0;

//        var initTxOF = numTransactions/2+1;
//        var orderFlightInput = Workload.createOrderFlightIterator(numTransactions/2, numRecords, numberOfAborts, initTxOF);
//        var initTxPB = 1;
//        var payBookingInput = Workload.createPayBookingIterator(numTransactions/2, initTxPB);

        var initTxOF = 1;
        var orderFlightInput = Workload.createOrderFlightIterator(numTransactions, numRecords, numberOfAborts, initTxOF, 0);
        var payBookingInput = Workload.createPayBookingIterator(0,0);

        this.orderFlightInput = orderFlightInput;
        this.payBookingInput = payBookingInput;
    }

    public void initExperiment(Properties coordinatorProperties) throws Exception
    {
        ComponentProcess.KillVMSes();
        System.out.println(STR."killed VMSes");

        System.console().readLine();
        System.out.println(STR."starting VMSes and coordinator");
        ComponentProcess.StartVMSes();
        Coordinator coordinator = loadCoordinator(coordinatorProperties);
        this.coordinator = coordinator;

        coordinator.registerBatchCommitConsumer((batchId, commitedTiDs) -> {
            BATCH_TO_FINISHED_TS_MAP.put(
                    batchId,
                    new BatchStats(batchId, commitedTiDs, System.currentTimeMillis()));
        });

        // keep a list
        coordinator.registerCrashConsumer((crashedVms) ->  {
            System.out.println(STR."Registering CRASH in experiment consumer");
            CRASH_MAP.putIfAbsent(crashedVms, new ArrayList<>());
            CRASH_MAP.get(crashedVms).add(System.currentTimeMillis());
        });

        // keep a list
        coordinator.registerCrashAckConsumer((crashedVms) ->  {
            System.out.println(STR."Registering CRASH ACK in experiment consumer");
            CRASH_ACK_MAP.putIfAbsent(crashedVms, new ArrayList<>());
            CRASH_ACK_MAP.get(crashedVms).add(System.currentTimeMillis());
        });

        coordinator.registerReconnectionConsumer((restartedVms) ->  {
            System.out.println(STR."Registering RECONNECTION in experiment consumer");
            RECONNECTION_MAP.putIfAbsent(restartedVms, new ArrayList<>());
            RECONNECTION_MAP.get(restartedVms).add(System.currentTimeMillis());
        });

        coordinator.registerReconnectionAckConsumer((restartedVms) ->  {
            System.out.println(STR."Registering RECONNECTION ACK in experiment consumer");
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

        // workload is submitted and processed by coordinator too quickly
        // just make two separate functions
        // return the thread
        // stop the order flight thread when flight crashes
        var orderFlightsThread = Workload.submitOrderFlights(orderFlightInput, coordinator, 1000, 35000);
//        var payBookingsThread = Workload.submitPayBookings(payBookingInput, coordinator, 1000, 35000);

        // coordinator has already sent the events ...
//        Util.Sleep(crashPoint);

        Util.Sleep(warmup);
        for (int i = 0; i < 4; i++) {
            ComponentProcess.Kill("payment");
            Util.Sleep(2500);
            ComponentProcess.StartVms("payment", true, 1);
            Util.Sleep(2500);
        }

//        ComponentProcess.Kill("payment");
//
////        try {
////            payBookingsThread.interrupt();
////        } catch (Exception e) {}
//
//        Util.Sleep(reconnectPoint-crashPoint);
//        ComponentProcess.StartVms("payment", true, 1);
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
        var totalTasks = 2*numIngestionWorkers;

        ExecutorService threadPool = Executors.newFixedThreadPool(totalTasks);
        BlockingQueue<Future<Void>> completionQueue = new ArrayBlockingQueue<>(totalTasks);
        CompletionService<Void> service = new ExecutorCompletionService<>(threadPool, completionQueue);

        var recordPerWorker = this.numRecords / this.numIngestionWorkers;

        long init = System.currentTimeMillis();
        long end;
        for (int i = 0; i < this.numIngestionWorkers; i++)
        {
            var startIdx = i*recordPerWorker;
            var endIdx = (i+1)*recordPerWorker;

            // first half of the flight seats and customers have been booked
//            var injectingBookings = i < numIngestionWorkers/2;
            var injectingBookings = false;

            service.submit(new IngestionWorkerCustomer(startIdx, endIdx, injectingBookings), null);
            service.submit(new IngestionWorkerFlight(startIdx, endIdx, injectingBookings), null);

            if (injectingBookings) {
                service.submit(new IngestionWorkerBooking(startIdx, endIdx), null);
            }
        }
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
        System.out.println(STR."ingesting \{this.numRecords} flight_seats and customers each with \{this.numIngestionWorkers} workers time: \{end-init}ms");

        // commit ingested data
        var client = HttpClient.newHttpClient();
        // Commit(client);
    }
    public static void Commit(HttpClient client)
    {
        HttpRequest cust_request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8769/customer/commit"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();

        HttpRequest fs_request = HttpRequest.newBuilder()
                .uri(URI.create(STR."http://localhost:8767/flight/commit"))
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();

        HttpRequest b_request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8768/booking/commit"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();

        try {
            client.send(cust_request, HttpResponse.BodyHandlers.ofString());
            client.send(fs_request, HttpResponse.BodyHandlers.ofString());
            client.send(b_request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            e.printStackTrace();
        }
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
