package dk.ku.di.dms.vms.flightScheduler.flight;

import dk.ku.di.dms.vms.flightScheduler.common.events.OrderFlight;
import dk.ku.di.dms.vms.flightScheduler.common.events.BookSeat;
import dk.ku.di.dms.vms.flightScheduler.flight.entities.FlightSeat;
import dk.ku.di.dms.vms.flightScheduler.flight.repositories.IFlightRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.Date;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;
@Microservice("flight")
public class FlightService
{
    private static final System.Logger LOGGER = System.getLogger(FlightService.class.getName());
    private final IFlightRepository flightRepository;
    public FlightService(IFlightRepository flightRepository){
        this.flightRepository = flightRepository;
    }

    @Inbound(values = {ORDER_FLIGHT})
    @Outbound(BOOK_SEAT)
    @Transactional(type=RW)
    public BookSeat orderingFlight(OrderFlight orderFlight)
    {
        if (orderFlight == null)
        {
            // abort
        }

        FlightSeat flightSeat = this.flightRepository.lookupByKey(new FlightSeat.FlightSeatId(orderFlight.flightId, orderFlight.seatNumber));

        if(flightSeat == null)
        {
            // abort
        }
        if (flightSeat.occupied != 1)
        {
            // abort
        }

        flightRepository.delete(flightSeat);
        return new BookSeat(new Date(), orderFlight);
    }
}
