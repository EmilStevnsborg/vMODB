package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

public class SegmentMetadata {
    public long nextFilePosition;
    public long bid;
    public int eventCount;
    public SegmentMetadata(long nextFilePosition, long bid, int eventCount) {
        this.nextFilePosition = nextFilePosition;
        this.bid = bid;
        this.eventCount = eventCount;
    }
}
