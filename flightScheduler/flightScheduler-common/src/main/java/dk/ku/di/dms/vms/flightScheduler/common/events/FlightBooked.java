package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class FlightBooked
{
    public int customerId;
    public FlightBooked(){}
    public FlightBooked(int customerId)
    {
        this.customerId = customerId;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "\"customerId\":" + customerId + "\n"
                + "}";
    }
}
