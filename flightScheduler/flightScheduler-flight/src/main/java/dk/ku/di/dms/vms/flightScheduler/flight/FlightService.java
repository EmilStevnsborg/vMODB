package dk.ku.di.dms.vms.flightScheduler.flight;

import dk.ku.di.dms.vms.flightScheduler.common.events.BookingCancelled;
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
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("flight")
public class FlightService
{
    private static final System.Logger LOGGER = System.getLogger(FlightService.class.getName());
    private final IFlightRepository flightRepository;
    public FlightService(IFlightRepository flightRepository){
        this.flightRepository = flightRepository;
    }

    // part of OrderFlight
    @Inbound(values = {ORDER_FLIGHT})
    @Outbound(BOOK_SEAT)
    @Transactional(type=RW)
    public BookSeat orderingFlight(OrderFlight orderFlight)
    {
        if (orderFlight == null)
        {
            throw new RuntimeException("orderFlight is null");
        }
        var flightSeatId = new FlightSeat.FlightSeatId(orderFlight.flight_id, orderFlight.seat_number);
        FlightSeat flightSeat = this.flightRepository.lookupByKey(flightSeatId);

        // throw exception to trigger abort
        if (flightSeat.occupied == 1)
        {
            throw new RuntimeException("flightSeat is occupied");
        }

        // update occupied status
        flightSeat.occupied = 1;
        this.flightRepository.update(flightSeat);

        return new BookSeat(new Date(), orderFlight);
    }

    // part of CancelBooking
    @Inbound(values = {BOOKING_CANCELLED})
    @Transactional(type=W)
    public void bookingCancelled(BookingCancelled bookingCancelled)
    {
        var flightSeat = new FlightSeat(bookingCancelled.flight_id, bookingCancelled.seat_number);
        flightRepository.update(flightSeat);
    }

}
