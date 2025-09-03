package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class OrderFlight
{
    public int customer_id;
    public int flight_id;
    public String seat_number;
    public OrderFlight(){}
    public OrderFlight(int customer_id, int flight_id, String seat_number)
    {
        this.customer_id = customer_id;
        this.flight_id = flight_id;
        this.seat_number = seat_number;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "  \"customer_id\":" + customer_id + ",\n"
                + "  \"flight_id\":" + flight_id + ",\n"
                + "  \"seat_number\":\"" + seat_number + "\"\n"
                + "}";
    }
}
