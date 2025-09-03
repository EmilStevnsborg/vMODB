package dk.ku.di.dms.vms.flightScheduler.booking.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.validation.constraints.Positive;
import java.util.Date;

@VmsTable(name="bookings")
public class Booking implements IEntity<Integer>
{
    public Booking(int booking_id, int customer_id, int flight_id, String seat_number, Date timestamp)
    {
        this.booking_id = booking_id;
        this.customer_id = customer_id;
        this.flight_id = flight_id;
        this.seat_number = seat_number;
        this.timestamp = timestamp;
        this.paid = 0;
    }
    public Booking(){}

    @Id
    @Positive
    public int booking_id;
    @Column
    public int customer_id;
    @Column
    public int flight_id;
    @Column
    public String seat_number;
    @Column
    public Date timestamp;
    @Column
    public int paid;

    public void BookingHasBeenPaid()
    {
        this.paid = 1;
    }

    @Override
    public String toString() {
        return "{\n"
                + "  \"booking_id\":" + booking_id + ",\n"
                + "  \"customerId\":" + customer_id + "\n"
                + "  \"flightId\":" + flight_id + ",\n"
                + "  \"seatNumber\":\"" + seat_number + "\",\n"
                + "  \"timestamp\":\"" + timestamp + "\",\n"
                + "  \"paid\":\"" + paid + "\"\n"
                + "}";
    }
}
