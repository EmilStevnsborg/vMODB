package dk.ku.di.dms.vms.modb.common.schema.network.transaction;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TransactionAbortAck
{
    public static final int SIZE = 1 + (2 * Long.BYTES);

    public static void write(ByteBuffer buffer, Payload payload) {
        buffer.put(Constants.TX_ABORT_ACK);
        buffer.putLong(payload.batch);
        buffer.putLong(payload.tid);
        buffer.putInt( payload.vms().length() );
        buffer.put( payload.vms().getBytes(StandardCharsets.UTF_8) );
    }

    public static void write(ByteBuffer buffer, long batch, long tid, VmsNode vmsIdentifier){
        buffer.put(Constants.TX_ABORT_ACK);
        buffer.putLong(batch);
        buffer.putLong(tid);
        byte[] nameBytes = vmsIdentifier.identifier.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( nameBytes.length );
        buffer.put( nameBytes );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        long tid = buffer.getLong();
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return new Payload(batch, tid, vms);
    }

    public static Payload of(long batch, long tid, String vms){
        return new Payload(batch, tid, vms);
    }

    public record Payload(
            long batch, long tid, String vms
    ) {
        @Override
        public String toString() {
            return "TransactionAbortAck {"
                    + "\"batch\":\"" + batch + "\""
                    + "\"tid\":\"" + tid + "\","
                    + "\"vms\":\"" + vms + "\""
                    + "}";
        }
    }

}