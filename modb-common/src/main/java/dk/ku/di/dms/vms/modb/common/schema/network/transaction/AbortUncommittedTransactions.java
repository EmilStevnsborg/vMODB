package dk.ku.di.dms.vms.modb.common.schema.network.transaction;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;

public class AbortUncommittedTransactions
{
    public static void write(ByteBuffer buffer) {
        buffer.put(Constants.ABORT_UNCOMMITTED_TRANSACTIONS);
    }

    public record Payload (){}
}
