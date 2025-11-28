package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class PaymentOrderIn
{
    public int o_id;
    public int o_d_id;
    public int o_w_id;
    public int amount;
    public PaymentOrderIn() {}
    public PaymentOrderIn(int o_id, int o_d_id, int o_w_id, int amount)
    {
        this.o_id = o_id;
        this.o_d_id = o_d_id;
        this.o_w_id = o_w_id;
        this.amount = amount;
    }
}
