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
import java.net.http.HttpClient;
import java.util.*;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.flightScheduler.proxy.Main.loadCoordinator;

public class BaselineExperiment
{
    private Coordinator coordinator;
    private final Map<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();
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

    public BaselineExperiment(int numRecords, int numTransactions, int numIngestionWorkers)
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

        System.console().readLine();
        System.out.println(STR."ingesting data");
        ingestData();
    }

    public ExperimentResults runExperiment(int runTime, int warmup) throws IOException
    {
        int newRuntime = runTime + warmup;
        var globalInitTs = System.currentTimeMillis();
        var orderFlightsThread = Workload.submitOrderFlights(orderFlightInput, coordinator, 1000, 40000);

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

        var results = ExperimentResults.Baseline(throughputInfo);
        writeResultsToFile(results);

        return results;
    }




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


    public static void writeResultsToFile(ExperimentResults results)
    {
        String fileName = "result_baseline.json";
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(fileName), results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
