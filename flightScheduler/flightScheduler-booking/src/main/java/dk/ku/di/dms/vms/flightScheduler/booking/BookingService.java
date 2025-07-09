package dk.ku.di.dms.vms.flightScheduler.booking;

import dk.ku.di.dms.vms.flightScheduler.booking.entities.Booking;
import dk.ku.di.dms.vms.flightScheduler.booking.repositories.IBookingRepository;
import dk.ku.di.dms.vms.flightScheduler.common.events.BookSeat;
import dk.ku.di.dms.vms.flightScheduler.common.events.SeatBooked;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("booking")
public class BookingService
{
    private static final System.Logger LOGGER = System.getLogger(BookingService.class.getName());
    private final IBookingRepository bookingRepository;
    public BookingService(IBookingRepository bookingRepository){
        this.bookingRepository = bookingRepository;
    }

    @Inbound(values = {BOOK_SEAT})
    @Outbound(SEAT_BOOKED)
    @Transactional(type=W)
    public SeatBooked seatBooking(BookSeat bookSeat)
    {
//        System.out.println(STR."bookSeat: \{bookSeat.toString()}");

        var order = bookSeat.orderFlight;
        var booking = new Booking(order.customerId, order.flightId, order.seatNumber, bookSeat.timestamp, 1);
        bookingRepository.insert(booking); // booking validity is verified by prior services

        var seatBooked = new SeatBooked(booking.next_booking_id, order.customerId, booking.flightId, booking.seatNumber, booking.timestamp);
        return seatBooked;
    }
}
