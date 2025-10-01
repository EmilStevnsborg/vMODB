package dk.ku.di.dms.vms.flightScheduler.flight.repositories;

import dk.ku.di.dms.vms.flightScheduler.flight.entities.FlightSeat;
import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

import java.util.List;

public interface IFlightRepository  extends IRepository<FlightSeat.FlightSeatId, FlightSeat> {
    @Query("select * from flight_seats where flight_id = :flight_id")
    List<FlightSeat> getFlightSeats(int flight_id);
}
