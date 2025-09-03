package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class CustomerReimbursed
{
    public int customer_id;

    public CustomerReimbursed(){}
    public CustomerReimbursed(int customer_id)
    {
        this.customer_id = customer_id;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "  \"customer_id\":" + customer_id + "\n"
                + "\n}";
    }
}
