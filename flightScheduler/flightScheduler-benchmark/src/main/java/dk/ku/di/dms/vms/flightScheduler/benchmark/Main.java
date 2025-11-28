package dk.ku.di.dms.vms.flightScheduler.benchmark;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.flightScheduler.benchmark.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.benchmark.experiment.Experiment;
import dk.ku.di.dms.vms.flightScheduler.benchmark.workload.Workload;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.flightScheduler.proxy.Main.loadCoordinator;
import static java.lang.System.Logger.Level.INFO;

public class Main
{

    public static int NUM_INGESTION_WORKERS = 5;
//            = Runtime.getRuntime().availableProcessors() / 4;
    public static int NUM_RECORDS = 100000;
    public static int NUM_TRANSACTIONS = 90000;
    private static final Properties PROPERTIES = ConfigUtils.loadProperties("src/main/resources/app.properties");

    public static void main(String[] args) throws Exception
    {
        try {
            ComponentProcess.KillVMSes();

            System.console().readLine();

            ComponentProcess.StartVMSes();
            Coordinator coordinator = loadCoordinator(PROPERTIES);

            // for injecting data i.e. num clients
            System.console().readLine();
            System.out.println(STR."ingesting data");

            ingestData();

            System.out.println(STR."data ingested");
            System.console().readLine();

            // experiment
            var runtime = 10000;
            var warmup = 1;

            var numberOfAborts = 1;
            var input = Workload.createOrderFlightIterator(NUM_TRANSACTIONS, NUM_RECORDS, numberOfAborts);
            Experiment.runAbortExperiment(coordinator, runtime, warmup, input);

        } catch (Exception e) {
            System.out.println("Failure starting components");
            e.printStackTrace();
        } finally {
            ComponentProcess.KillVMSes();
        }
    }

    private static void submitOrderFlightTransactions()
    {

    }

    // store it persistently in
    // default data
    private static void ingestData()
    {
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_INGESTION_WORKERS);
        BlockingQueue<Future<Void>> completionQueue = new ArrayBlockingQueue<>(NUM_INGESTION_WORKERS);
        CompletionService<Void> service = new ExecutorCompletionService<>(threadPool, completionQueue);

        var recordPerWorker = NUM_RECORDS / NUM_INGESTION_WORKERS;

        long init = System.currentTimeMillis();
        long end;
        for (int i = 0; i < NUM_INGESTION_WORKERS; i++)
        {
            service.submit(new IngestionWorker("customer", i*recordPerWorker, (i+1)*recordPerWorker), null);
        }
        for (int i = 0; i < NUM_INGESTION_WORKERS; i++)
        {
            service.submit(new IngestionWorker("flight", i*recordPerWorker, (i+1)*recordPerWorker), null);
        }
        try {
            for (int i = 0; i < NUM_INGESTION_WORKERS; i++) {
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
        System.out.println(STR."ingesting \{NUM_RECORDS} flight_seats and customers each with \{NUM_INGESTION_WORKERS} workers time: \{end-init}ms");

        // commit ingested data
        var client = HttpClient.newHttpClient();
        Commit(client);
    }
    public static void Commit(HttpClient client)
    {
        HttpRequest cust_request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8769/customer/commit"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(""))
                .build();

        HttpRequest fs_request = HttpRequest.newBuilder()
                .uri(URI.create(STR."http://localhost:8767/flight/commit"))
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(""))
                .build();

        try {
            client.send(cust_request, HttpResponse.BodyHandlers.ofString());
            client.send(fs_request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // read it from files
    // send over minimal client
    // use multiple thread
    // finish with a commit/checkpoint
    public static void SubmitInjectionData()
    {
        // read from table
    }

    public void InjectDataIntoTables()
    {
        // through proxy
        // make sure to flush
    }

    public void createFlightOrderWorkload(int orders)
    {

    }
}
