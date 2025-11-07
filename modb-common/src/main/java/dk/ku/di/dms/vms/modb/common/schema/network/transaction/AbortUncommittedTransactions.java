package dk.ku.di.dms.vms.modb.common.schema.network.transaction;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;

public class AbortUncommittedTransactions
{
    public static void write(ByteBuffer buffer, long bid) {
        buffer.put(Constants.ABORT_UNCOMMITTED_TRANSACTIONS);
        buffer.putLong(bid);
    }
    public static AbortUncommittedTransactions.Payload read(ByteBuffer buffer) {
        var bid = buffer.getLong();
        return new AbortUncommittedTransactions.Payload(bid);
    }

    public record Payload ( long bid ){}
}
