package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ReconnectionAck
{

    public static void write(ByteBuffer buffer, String vmsRestarted, String vmsAcknowledged)
    {
        buffer.put(Constants.RECONNECTIONS_ACKNOWLEDGEMENT);
        buffer.putInt( vmsRestarted.length() );
        buffer.put( vmsRestarted.getBytes(StandardCharsets.UTF_8) );
        buffer.putInt( vmsAcknowledged.length() );
        buffer.put( vmsAcknowledged.getBytes(StandardCharsets.UTF_8) );
    }

    public static ReconnectionAck.Payload read(ByteBuffer buffer){
        int size1 = buffer.getInt();
        String vmsRestarted = ByteUtils.extractStringFromByteBuffer(buffer, size1);
        int size2 = buffer.getInt();
        String vmsAcknowledged = ByteUtils.extractStringFromByteBuffer(buffer, size2);
        return ReconnectionAck.of(vmsRestarted, vmsAcknowledged);
    }

    public static ReconnectionAck.Payload of(String vmsRestarted, String vmsAcknowledged) {
        return new ReconnectionAck.Payload(vmsRestarted, vmsAcknowledged);
    }

    public record Payload(
            String vmsRestarted,
            String vmsAcknowledged
    ) {
        @Override
        public String toString() {
            return "Reconnection acknowledgement {"
                    + "\"vmsRestarted\":\"" + vmsRestarted + "\","
                    + "\"vmsAcknowledged\":\"" + vmsAcknowledged + "\""
                    + "}";
        }
    }
}
