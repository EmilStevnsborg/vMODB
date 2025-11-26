package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class PayBooking
{
    public int booking_id;
    public String payment_method;
    public PayBooking(){}
    public PayBooking(int booking_id, String payment_method)
    {
        this.booking_id = booking_id;
        this.payment_method = payment_method;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "  \"booking_id\":" + booking_id + ",\n"
                + "  \"payment_method\":\"" + payment_method + "\"\n"
                + "}";
    }
}
