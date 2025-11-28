package dk.ku.di.dms.vms.flightScheduler.benchmark;

import dk.ku.di.dms.vms.flightScheduler.benchmark.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.benchmark.models.FlightSeat;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiFunction;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.CUSTOMER_VMS_PORT;
import static dk.ku.di.dms.vms.flightScheduler.common.Constants.FLIGHT_VMS_PORT;
import static java.lang.System.Logger.Level.ERROR;

public class IngestionWorker implements Runnable
{
    private String dataType;
    private int startIdx;
    private int endIdx;
    public IngestionWorker(String dataType, int startIdx, int endIdx)
    {
        this.dataType = dataType;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }
    private static final BiFunction<String, Integer, MinimalHttpClient> HTTP_CLIENT_SUPPLIER = (host, port) ->
    {
        try {
            return new MinimalHttpClient(host, port);
        } catch (Exception e) {
            throw new RuntimeException("");
        }
    };

    @Override
    public void run()
    {
        try {
            // System.out.println(STR."ingestion worker run");
            var port = dataType.equals("customer") ? (int) CUSTOMER_VMS_PORT : (int) FLIGHT_VMS_PORT;
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply("localhost", port);

            for (int i = startIdx; i < endIdx; i++)
            {
                switch (dataType)
                {
                    case "customer" -> {
                        var customer = new Customer(i, 100, STR."user_\{i}");
                        client.sendRequest("POST", customer.toString(), dataType);
                    }
                    case "flight" -> {
                        int number = i;
                        var seat = new FlightSeat(0, number);
                        client.sendRequest("POST", seat.toString(), dataType);
                    }
                }
            }

            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
