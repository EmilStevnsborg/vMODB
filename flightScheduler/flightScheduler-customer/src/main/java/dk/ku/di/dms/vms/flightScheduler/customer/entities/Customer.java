package dk.ku.di.dms.vms.flightScheduler.customer.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;

@VmsTable(name="customers")
public final class Customer implements IEntity<Integer> {

    public Customer(int customerId, String name)
    {
        this.customerId = customerId;
        this.name = name;
    }

    @SuppressWarnings("unused")
    public Customer(){}

    @Id
    public int customerId;
    @Column
    public String name;


    @Override
    public String toString() {
        return "{\n"
                + "  \"customerId\":" + customerId + ",\n"
                + "  \"name\":\"" + name + "\"\n"
                + "}";
        }
}
