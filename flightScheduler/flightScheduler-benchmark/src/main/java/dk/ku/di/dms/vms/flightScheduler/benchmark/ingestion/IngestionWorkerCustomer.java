package dk.ku.di.dms.vms.flightScheduler.benchmark.ingestion;

import dk.ku.di.dms.vms.flightScheduler.benchmark.MinimalHttpClient;
import dk.ku.di.dms.vms.flightScheduler.benchmark.models.Customer;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.CUSTOMER_VMS_PORT;

public class IngestionWorkerCustomer extends IngestionWorker
{
    private boolean bookedSeat;
    public IngestionWorkerCustomer(int startIdx, int endIdx, boolean bookedSeat) {
        super(startIdx, endIdx);
        this.bookedSeat = bookedSeat;
    }

    @Override
    public void run()
    {
        try
        {
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply("localhost", CUSTOMER_VMS_PORT);
            for (int i = startIdx; i < endIdx; i++) {
                var customer = new Customer(i, 100, STR."user_\{i}");
                if (bookedSeat) {
                    customer.seat_number = i;
                }
                client.sendRequest("POST", customer.toString(), "customer");
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
