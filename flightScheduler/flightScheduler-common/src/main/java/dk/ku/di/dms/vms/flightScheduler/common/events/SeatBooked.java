package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.Date;

@Event
public class SeatBooked
{
    public int booking_id;
    public int flight_id;
    public int seat_number;
    public int customer_id;
    public Date timestamp;

    public SeatBooked() {}
    public SeatBooked(int booking_id, int customer_id, int flight_id, int seat_number, Date timestamp)
    {
        this.booking_id = booking_id;
        this.customer_id = customer_id;
        this.flight_id = flight_id;
        this.seat_number = seat_number;
        this.timestamp = timestamp;
    }
    @Override
    public String toString()
    {
        return "{\n"
                + "  \"booking_id\":" + booking_id + ",\n"
                + "  \"customer_id\":" + customer_id + ",\n"
                + "  \"flight_id\":" + flight_id + ",\n"
                + "  \"seat_number\":\"" + seat_number + "\",\n"
                + "  \"timestamp\":" + timestamp + "\n"
                + "}";
    }
}
