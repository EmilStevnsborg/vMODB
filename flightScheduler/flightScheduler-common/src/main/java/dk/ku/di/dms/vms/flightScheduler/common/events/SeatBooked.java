package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.Date;

@Event
public class SeatBooked
{
    public int bookingId;
    public int flightId;
    public String seatNumber;
    public int customerId;
    public Date timestamp;

    public SeatBooked() {}
    public SeatBooked(int bookingId, int customerId, int flightId, String seatNumber, Date timestamp)
    {
        this.bookingId = bookingId;
        this.customerId = customerId;
        this.flightId = flightId;
        this.seatNumber = seatNumber;
        this.timestamp = timestamp;
    }
    @Override
    public String toString()
    {
        return "{\n"
                + "  \"bookingId\":\"" + bookingId + "\",\n"
                + "  \"customerId\":\"" + customerId + "\",\n"
                + "  \"flightId\":\"" + flightId + "\",\n"
                + "  \"seatNumber\":\"" + seatNumber + "\",\n"
                + "  \"seatNumber\":\"" + seatNumber + "\",\n"
                + "  \"timestamp\":\"" + timestamp.toString() + "\"\n"
                + "}";
    }
}
