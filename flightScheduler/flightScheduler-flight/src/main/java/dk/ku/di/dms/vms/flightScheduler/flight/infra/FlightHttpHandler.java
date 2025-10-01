package dk.ku.di.dms.vms.flightScheduler.flight.infra;

import dk.ku.di.dms.vms.flightScheduler.flight.entities.FlightSeat;
import dk.ku.di.dms.vms.flightScheduler.flight.repositories.IFlightRepository;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;

import java.util.List;
import java.util.stream.Collectors;

public class FlightHttpHandler extends DefaultHttpHandler
{
    private final IFlightRepository repository;
    public FlightHttpHandler(ITransactionManager transactionManager,
                             IFlightRepository repository){
        super(transactionManager);
        this.repository = repository;
        System.out.println(STR."\nRepo is \{repository}\n");
    }

    @Override
    public void post(String uri, String payload)
    {
        FlightSeat flightSeat = SERDES.deserialize(payload, FlightSeat.class);
        this.transactionManager.beginTransaction(0, 0, 0, false);
        this.repository.upsert(flightSeat); // upsert: update + insert (if it exists, update, else insert)
    }

    // http://host/flight/{id}
    @Override
    public String getAsJson(String uri) {
        System.out.println(STR."Getting flight seats");
        String[] split = uri.split("/");
        int flight_id = Integer.parseInt(split[split.length - 1]);
        this.transactionManager.beginTransaction(-1, -1, -1,true);
        var flightSeats = this.repository.getFlightSeats(flight_id);

        return flightSeats.toString();
    }
}