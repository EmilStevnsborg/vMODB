package dk.ku.di.dms.vms.coordinator.batch;

public class TxWorkerProcessedCrash
{
    public final int id;
    public final String vms;
    public TxWorkerProcessedCrash(int id, String vms)
    {
        this.id = id;
        this.vms = vms;
    }
}
