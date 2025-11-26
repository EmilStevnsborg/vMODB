package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class CustomerReimbursed
{
    public int customer_id;
    public int reimbursement;

    public CustomerReimbursed(){}
    public CustomerReimbursed(int customer_id, int reimbursement)
    {
        this.customer_id = customer_id;
        this.reimbursement = reimbursement;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "  \"customer_id\":" + customer_id + ",\n"
                + "  \"reimbursement\":" + reimbursement + "\n"
                + "\n}";
    }
}
