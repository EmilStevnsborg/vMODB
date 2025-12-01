package dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion;

import dk.ku.di.dms.vms.flightScheduler.benchmark.MinimalHttpClient;
import dk.ku.di.dms.vms.flightScheduler.benchmark.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.benchmark.models.FlightSeat;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.CUSTOMER_VMS_PORT;
import static dk.ku.di.dms.vms.flightScheduler.common.Constants.FLIGHT_VMS_PORT;

public class IngestionWorkerFlight extends IngestionWorker
{
    private boolean occupied;
    public IngestionWorkerFlight(int startIdx, int endIdx, boolean occupied) {
        super(startIdx, endIdx);
        this.occupied = occupied;
    }

    @Override
    public void run()
    {
        try
        {
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply("localhost", FLIGHT_VMS_PORT);
            for (int i = startIdx; i < endIdx; i++) {
                var flightSeat = new FlightSeat(0, i);
                if (occupied) {
                    flightSeat.occupied = 1;
                }
                client.sendRequest("POST", flightSeat.toString(), "flight");
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
