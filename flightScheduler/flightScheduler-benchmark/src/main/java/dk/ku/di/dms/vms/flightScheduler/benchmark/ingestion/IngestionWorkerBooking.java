package dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion;

import dk.ku.di.dms.vms.flightScheduler.benchmark.MinimalHttpClient;
import dk.ku.di.dms.vms.flightScheduler.benchmark.models.Booking;
import dk.ku.di.dms.vms.flightScheduler.benchmark.models.Customer;

import java.util.Date;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.BOOKING_VMS_PORT;
import static dk.ku.di.dms.vms.flightScheduler.common.Constants.CUSTOMER_VMS_PORT;

public class IngestionWorkerBooking extends IngestionWorker
{
    public IngestionWorkerBooking(int startIdx, int endIdx) {
        super(startIdx, endIdx);
    }

    @Override
    public void run()
    {
        try
        {
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply("localhost", BOOKING_VMS_PORT);
            for (int i = startIdx; i < endIdx; i++) {
                var booking = new Booking(-1,i,0,i,new Date().toString(), 20);
                client.sendRequest("POST", booking.toString(), "booking");
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}