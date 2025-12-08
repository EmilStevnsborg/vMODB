package dk.ku.di.dms.vms.tpcc.benchmark;

import dk.ku.di.dms.vms.tpcc.benchmark.Util.ComponentProcess;
import dk.ku.di.dms.vms.tpcc.benchmark.experiment.AbortExperiment;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.util.Properties;

public class Main
{
    public static int NUM_TRANSACTIONS = 500000; // 1.5 mil
    private static final Properties PROPERTIES = ConfigUtils.loadProperties("src/main/resources/app.properties");

    public static void main(String[] args) throws Exception
    {
        try {
//            var experiment = new BaselineExperiment(NUM_RECORDS, NUM_TRANSACTIONS, NUM_INGESTION_WORKERS);
            var experiment = new AbortExperiment(NUM_TRANSACTIONS);
//            var experiment = new RecoverVmsExperiment(NUM_RECORDS, NUM_TRANSACTIONS, NUM_INGESTION_WORKERS);

            experiment.initExperiment(PROPERTIES);

            // for injecting data i.e. num clients
            System.out.println(STR."ENTER to start experiment");
            System.console().readLine();

            // experiment
            var runtime = 20000;
            var warmup = 1;

            experiment.runExperiment(runtime, warmup);

        } catch (Exception e) {
            System.out.println("Failure starting components");
            e.printStackTrace();
        } finally {
            ComponentProcess.KillVMSes();
        }
    }

}
