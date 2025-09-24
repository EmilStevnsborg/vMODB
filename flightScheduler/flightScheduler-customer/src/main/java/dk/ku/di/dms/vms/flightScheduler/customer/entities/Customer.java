package dk.ku.di.dms.vms.flightScheduler.customer.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;

@VmsTable(name="customers")
public final class Customer implements IEntity<Integer> {

    @Id
    public int customer_id;
    @Column
    public int money;
    @Column
    public String name;

    public Customer(int customer_id, int money, String name)
    {
        this.customer_id = customer_id;
        this.money = money;
        this.name = name;
    }

    @SuppressWarnings("unused")
    public Customer(){}


    @Override
    public String toString() {
        return "{\n"
                + "  \"customer_id\":" + customer_id + ",\n"
                + "  \"money\":" + money + ",\n"
                + "  \"name\":\"" + name + "\"\n"
                + "}";
        }
}
