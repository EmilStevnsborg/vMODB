package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class OrderFlight
{
    public int customerId;
    public int flightId;
    public String seatNumber;
    public OrderFlight(){}
    public OrderFlight(int customerId, int flightId, String seatNumber)
    {
        this.customerId = customerId;
        this.flightId = flightId;
        this.seatNumber = seatNumber;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "  \"customerId\":" + customerId + ",\n"
                + "  \"flightId\":" + flightId + ",\n"
                + "  \"seatNumber\":\"" + seatNumber + "\"\n"
                + "}";
    }
}
