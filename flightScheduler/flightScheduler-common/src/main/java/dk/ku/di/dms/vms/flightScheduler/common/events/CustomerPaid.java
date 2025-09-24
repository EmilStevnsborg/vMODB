package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class CustomerPaid
{
    public int customer_id;
    public int price;
    public CustomerPaid(){}
    public CustomerPaid(int customer_id, int price)
    {
        this.customer_id = customer_id;
        this.price = price;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "  \"customer_id\":" + customer_id + ",\n"
                + "  \"price\":" + price + "\n"
                + "}";
    }
}
