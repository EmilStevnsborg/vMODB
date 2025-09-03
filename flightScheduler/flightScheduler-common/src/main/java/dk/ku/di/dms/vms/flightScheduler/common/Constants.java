package dk.ku.di.dms.vms.flightScheduler.common;

public class Constants
{
    public static final int FLIGHT_VMS_PORT = 8767;

    public static final int BOOKING_VMS_PORT = 8768;

    public static final int CUSTOMER_VMS_PORT = 8769;
    public static final int PAYMENT_VMS_PORT = 8770;

    // order flight
    public static final String ORDER_FLIGHT = "order_flight";
    public static final String BOOK_SEAT = "book_seat";
    public static final String SEAT_BOOKED = "seat_booked";

    // pay booking
    public static final String PAY_BOOKING = "pay_booking";
    public static final String PAYMENT_SUCCEEDED = "payment_succeeded";

    // cancel booking
    public static final String CANCEL_BOOKING = "cancel_booking";
    public static final String BOOKING_CANCELLED = "booking_cancelled";

    // reimburse customer
    public static final String REIMBURSE_BOOKING = "reimburse_booking";
    public static final String CUSTOMER_REIMBURSED = "customer_reimbursed";
}
