package dk.ku.di.dms.vms.flightScheduler.payment.entities;


import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.validation.constraints.Positive;

@VmsTable(name="payments")
public class Payment implements IEntity<Integer>
{
    @Id
    @Positive
    public int booking_id;

    @Column
    public String payment_method;

    public Payment() {}
    public Payment(int booking_id, String payment_method)
    {
        this.booking_id = booking_id;
        this.payment_method = payment_method;
    }
}
