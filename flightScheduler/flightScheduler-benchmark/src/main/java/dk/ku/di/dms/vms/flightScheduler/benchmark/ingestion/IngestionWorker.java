package dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion;

import dk.ku.di.dms.vms.flightScheduler.benchmark.MinimalHttpClient;
import dk.ku.di.dms.vms.flightScheduler.benchmark.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.benchmark.models.FlightSeat;

import java.util.function.BiFunction;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.CUSTOMER_VMS_PORT;
import static dk.ku.di.dms.vms.flightScheduler.common.Constants.FLIGHT_VMS_PORT;

public abstract class IngestionWorker implements Runnable
{
    protected int startIdx;
    protected int endIdx;
    public IngestionWorker(int startIdx, int endIdx)
    {
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }
    protected static BiFunction<String, Integer, MinimalHttpClient> HTTP_CLIENT_SUPPLIER = (host, port) ->
    {
        try {
            return new MinimalHttpClient(host, port);
        } catch (Exception e) {
            throw new RuntimeException("");
        }
    };
}
