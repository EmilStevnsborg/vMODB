package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbortAck;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CrashAck
{
    public static void write(ByteBuffer buffer, CrashAck.Payload payload) {
        buffer.put(Constants.CRASH_ACK);
        buffer.putInt( payload.vmsCrashed.length() );
        buffer.put( payload.vmsCrashed.getBytes(StandardCharsets.UTF_8) );
        buffer.putInt( payload.vmsAcknowledged.length() );
        buffer.put( payload.vmsAcknowledged.getBytes(StandardCharsets.UTF_8) );
    }
    public static void write(ByteBuffer buffer, String vmsCrashed, String vmsAcknowledged)
    {
        buffer.put(Constants.CRASH_ACK);
        buffer.putInt( vmsCrashed.length() );
        buffer.put( vmsCrashed.getBytes(StandardCharsets.UTF_8) );
        buffer.putInt( vmsAcknowledged.length() );
        buffer.put( vmsAcknowledged.getBytes(StandardCharsets.UTF_8) );
    }

    public static CrashAck.Payload read(ByteBuffer buffer){
        int size1 = buffer.getInt();
        String vmsCrashed = ByteUtils.extractStringFromByteBuffer(buffer, size1);
        int size2 = buffer.getInt();
        String vmsAcknowledged = ByteUtils.extractStringFromByteBuffer(buffer, size2);
        return CrashAck.of(vmsCrashed, vmsAcknowledged);
    }

    public static CrashAck.Payload of(String vmsCrashed, String vmsAcknowledged) {
        return new CrashAck.Payload(vmsCrashed, vmsAcknowledged);
    }

    public record Payload(
            String vmsCrashed,
            String vmsAcknowledged
    ) {
        @Override
        public String toString() {
            return "Crash acknowledgement {"
                    + "\"vmsCrashed\":\"" + vmsCrashed + "\","
                    + "\"vmsAcknowledged\":\"" + vmsAcknowledged + "\""
                    + "}";
        }
    }
}
