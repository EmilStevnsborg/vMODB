package dk.ku.di.dms.vms.flightScheduler.benchmark.experiment;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.flightScheduler.common.events.OrderFlight;
import dk.ku.di.dms.vms.flightScheduler.common.events.PayBooking;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class RecoverCoordinatorExperiment
{
    private Coordinator coordinator;
    private final Map<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();
    private final Random generate = new Random();
    private final int numRecords;
    private final int numIngestionWorkers;

    public int randomNumber(int min, int max) {
        int next = generate.nextInt();
        int div = next % ((max - min) + 1);
        if (div < 0) {
            div = div * -1;
        }
        return min + div;
    }

    public RecoverCoordinatorExperiment(int numRecords, int numTransactions, int numIngestionWorkers)
    {
        this.numRecords = numRecords;
        this.numIngestionWorkers = numIngestionWorkers;
    }

    public void initExperiment(Properties coordinatorProperties) throws Exception {
    }
}
