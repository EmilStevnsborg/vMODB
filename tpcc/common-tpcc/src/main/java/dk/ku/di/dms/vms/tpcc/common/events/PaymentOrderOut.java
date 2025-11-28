package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class PaymentOrderOut
{
    public int c_id;
    public int d_id;
    public int w_id;
    public int amount;
    public PaymentOrderOut() {}
    public PaymentOrderOut(int c_id, int d_id, int w_id, int amount)
    {
        this.c_id = c_id;
        this.d_id = d_id;
        this.w_id = w_id;
        this.amount = amount;
    }
}
