package dk.ku.di.dms.vms.flightScheduler.test.models;

public class Customer
{
    public final int customer_id;
    public final int money;
    public final String name;

    public Customer(int customer_id, int money, String name) {
        this.customer_id = customer_id;
        this.money = money;
        this.name = name;
    }
    @Override
    public String toString()
    {
        return "{"
                + "  \"customer_id\":\"" + customer_id + "\","
                + "  \"money\":" + money + ",\n"
                + "  \"name\":\"" + name + "\""
                + "\n}";
    }
}
