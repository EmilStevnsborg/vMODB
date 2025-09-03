package dk.ku.di.dms.vms.flightScheduler.test.models;

public class Customer
{
    public final int customer_id;
    public final String name;

    public Customer(int customer_id, String name) {
        this.customer_id = customer_id;
        this.name = name;
    }
    @Override
    public String toString()
    {
        return "{"
                + "  \"customer_id\":\"" + customer_id + "\","
                + "  \"name\":\"" + name + "\""
                + "\n}";
    }
}
