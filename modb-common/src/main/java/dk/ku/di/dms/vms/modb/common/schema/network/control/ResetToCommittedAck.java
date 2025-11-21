package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbortInfo;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ResetToCommittedAck
{
    public static void write(ByteBuffer buffer, ResetToCommittedAck.Payload payload) {
        buffer.put(Constants.RESET_TO_COMMITTED_ACK);
        buffer.putInt( payload.vms().length() );
        buffer.put( payload.vms().getBytes(StandardCharsets.UTF_8) );
    }

    public static void write(ByteBuffer buffer, String vms){
        buffer.put(Constants.RESET_TO_COMMITTED_ACK);
        byte[] nameBytes = vms.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( nameBytes.length );
        buffer.put( nameBytes );
    }

    public static ResetToCommittedAck.Payload read(ByteBuffer buffer){
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return new ResetToCommittedAck.Payload(vms);
    }

    public static ResetToCommittedAck.Payload of(String vms){
        return new ResetToCommittedAck.Payload(vms);
    }

    public record Payload(
            String vms
    ) {
        @Override
        public String toString() {
            return "ResetToCommittedAck {"
                    + "\"vms\":\"" + vms + "\""
                    + "}";
        }
    }
}
