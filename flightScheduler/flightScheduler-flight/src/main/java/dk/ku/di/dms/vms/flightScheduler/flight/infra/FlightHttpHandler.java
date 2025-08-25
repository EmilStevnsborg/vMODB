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
        System.out.println(STR."\nPosting \{payload}\n");
        FlightSeat flightSeat = SERDES.deserialize(payload, FlightSeat.class);
        this.transactionManager.beginTransaction(0, 0, 0, false);
        this.repository.upsert(flightSeat); // upsert: update + insert (if it exists, update, else insert)
    }

    // http://host/flight/{id}
    @Override
    public String getAsJson(String uri) {
        String[] split = uri.split("/");
        int flightId = Integer.parseInt(split[split.length - 1]);
        this.transactionManager.beginTransaction(0, 0, 0,true);
        var flightSeats = this.repository.getFlightSeats(flightId)
                .stream()
                .map(FlightSeat::toString)
                .collect(Collectors.toList());

        return flightSeats.toString();
    }
}