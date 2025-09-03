package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class CancelBooking
{
    public int booking_id;

    public CancelBooking(){}
    public CancelBooking(int booking_id)
    {
        this.booking_id = booking_id;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "  \"booking_id\":" + booking_id + "\n"
                + "\n}";
    }
}
