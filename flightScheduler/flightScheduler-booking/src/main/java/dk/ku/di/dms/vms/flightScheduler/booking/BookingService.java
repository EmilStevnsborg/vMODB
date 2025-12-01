package dk.ku.di.dms.vms.flightScheduler.booking;

import dk.ku.di.dms.vms.flightScheduler.booking.entities.Booking;
import dk.ku.di.dms.vms.flightScheduler.booking.repositories.IBookingRepository;
import dk.ku.di.dms.vms.flightScheduler.common.events.*;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("booking")
public class BookingService
{
    private static final System.Logger LOGGER = System.getLogger(BookingService.class.getName());
    public static final AtomicInteger booking_counter = new AtomicInteger(0);
    private final IBookingRepository bookingRepository;
    public BookingService(IBookingRepository bookingRepository){
        this.bookingRepository = bookingRepository;
    }

    // part of OrderFlight
    @Inbound(values = {BOOK_SEAT})
    @Outbound(SEAT_BOOKED)
    @Transactional(type=W)
    public SeatBooked seatBooking(BookSeat bookSeat)
    {
//        if (bookSeat.toString() != null) throw new RuntimeException();
        var order = bookSeat.orderFlight;
        var booking_id = booking_counter.incrementAndGet();
        var price = 20;
        var booking = new Booking(booking_id, order.customer_id, order.flight_id, order.seat_number, bookSeat.timestamp, price);
        bookingRepository.insert(booking); // booking validity is verified by prior services

        var seatBooked = new SeatBooked(booking.booking_id, order.customer_id, booking.flight_id, booking.seat_number, booking.timestamp);
        return seatBooked;
    }

    // part of PayBooking
    @Inbound(values = {PAYMENT_SUCCEEDED})
    @Transactional(type=RW)
    @Outbound(CUSTOMER_PAID)
    public CustomerPaid bookingHasBeenPaid(PaymentSucceeded paymentSucceeded)
    {
        var booking = bookingRepository.lookupByKey(paymentSucceeded.booking_id);
        booking.bookingHasBeenPaid();
        bookingRepository.update(booking);

        var customerPaid = new CustomerPaid(booking.customer_id, booking.price);
        return customerPaid;
    }

    // part of CancelBooking
    @Inbound(values = {CANCEL_BOOKING})
    @Transactional(type=RW)
    @Outbound(BOOKING_CANCELLED)
    public BookingCancelled cancelBooking(CancelBooking cancelBooking)
    {
        var bookingObject = bookingRepository.lookupByKey(cancelBooking.booking_id);
        bookingRepository.deleteByKey(cancelBooking.booking_id);

        // requires reimbursement, if it has been paid
        var bookingCancelled = new BookingCancelled(cancelBooking.booking_id, bookingObject.paid,
                                                    bookingObject.customer_id, bookingObject.flight_id,
                                                    bookingObject.seat_number, bookingObject.price);
        return bookingCancelled;
    }
}
