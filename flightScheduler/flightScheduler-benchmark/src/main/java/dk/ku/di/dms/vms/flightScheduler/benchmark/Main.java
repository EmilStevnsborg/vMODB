package dk.ku.di.dms.vms.flightScheduler.benchmark;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.flightScheduler.benchmark.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.benchmark.experiment.AbortExperiment;
import dk.ku.di.dms.vms.flightScheduler.benchmark.experiment.BaselineExperiment;
import dk.ku.di.dms.vms.flightScheduler.benchmark.experiment.RecoverVmsExperiment;
import dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion.IngestionWorker;
import dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion.IngestionWorkerCustomer;
import dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion.IngestionWorkerFlight;
import dk.ku.di.dms.vms.flightScheduler.benchmark.workload.Workload;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.flightScheduler.proxy.Main.loadCoordinator;

public class Main
{

    public static int NUM_INGESTION_WORKERS = 4;
//            = Runtime.getRuntime().availableProcessors() / 4;
    public static int NUM_RECORDS = 1000000; // 1.5 mil (10 workers 17.135s-25.235s)
    public static int NUM_TRANSACTIONS = 1000000; // 1.5 mil
    private static final Properties PROPERTIES = ConfigUtils.loadProperties("src/main/resources/app.properties");

    public static void main(String[] args) throws Exception
    {
        try {
//            var experiment = new BaselineExperiment(NUM_RECORDS, NUM_TRANSACTIONS, NUM_INGESTION_WORKERS);
            var experiment = new AbortExperiment(NUM_RECORDS, NUM_TRANSACTIONS, NUM_INGESTION_WORKERS);
//            var experiment = new RecoverVmsExperiment(NUM_RECORDS, NUM_TRANSACTIONS, NUM_INGESTION_WORKERS);

            experiment.initExperiment(PROPERTIES);

            // for injecting data i.e. num clients
            System.out.println(STR."ENTER to start experiment");
            System.console().readLine();

            // experiment
            var runtime = 25000;
            var warmup = 3000;

            experiment.runExperiment(runtime, warmup);

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

}
