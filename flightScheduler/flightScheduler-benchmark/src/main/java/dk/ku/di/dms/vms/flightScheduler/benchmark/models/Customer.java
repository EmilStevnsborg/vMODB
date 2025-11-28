package dk.ku.di.dms.vms.flightScheduler.benchmark.models;

public class Customer
{
    public int customer_id;
    public int money;
    public String name;
    public int seat_number;

    public Customer(int customer_id, int money, String name) {
        this.customer_id = customer_id;
        this.money = money;
        this.name = name;
    }

    public Customer(){}

    @Override
    public String toString()
    {
        return "{"
                + "  \"customer_id\":\"" + customer_id + "\","
                + "  \"money\":\"" + money + "\",\n"
                + "  \"name\":\"" + name + "\",\n"
                + "  \"seat_number\":\"" + seat_number + "\""
                + "\n}";
    }
}
