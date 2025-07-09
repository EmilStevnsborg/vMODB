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
    public Booking(int customerId, int flightId, String seatNumber, Date timestamp, int next_booking_id)
    {
        this.next_booking_id = next_booking_id;
        this.customerId = customerId;
        this.flightId = flightId;
        this.seatNumber = seatNumber;
        this.timestamp = timestamp;
    }
    public Booking(){}

    @Id
    @Positive
    public int next_booking_id;
    @Column
    public int customerId;
    @Column
    public int flightId;
    @Column
    public String seatNumber;
    @Column
    public Date timestamp;
}
