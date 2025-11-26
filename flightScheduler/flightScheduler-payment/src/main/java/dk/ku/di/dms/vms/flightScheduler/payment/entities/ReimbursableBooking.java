package dk.ku.di.dms.vms.flightScheduler.payment.entities;


import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.validation.constraints.Positive;

@VmsTable(name="bookings_to_reimburse")
public class ReimbursableBooking implements IEntity<Integer>
{
    @Id
    @Positive
    public int booking_id;

    @Column
    public int customer_id;
    @Column
    public int reimbursement;

    public ReimbursableBooking(){}

    public ReimbursableBooking(int booking_id, int customer_id, int reimbursement)
    {
        this.booking_id = booking_id;
        this.customer_id = customer_id;
        this.reimbursement = reimbursement;
    }
}
