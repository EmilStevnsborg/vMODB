package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VmsReconnect
{
    public static void write(ByteBuffer buffer, String vms, long vmsLastTid)
    {
        buffer.put(Constants.VMS_RECONNECTION);
        buffer.putInt( vms.length() );
        buffer.put( vms.getBytes(StandardCharsets.UTF_8) );
        buffer.putLong( vmsLastTid );
    }

    public static Payload read(ByteBuffer buffer){
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        long lastTid = buffer.getLong();
        return VmsReconnect.of(vms,  lastTid);
    }

    public static Payload of(String vms, long vmsLastTid) {
        return new Payload(vms, vmsLastTid);
    }

    public record Payload(
            String vms, long vmsLastTid
    ) {
        @Override
        public String toString() {
            return "VmsReconnect {"
                    + "\"vms\":\"" + vms + "\""
                    + ",\"vmsLastTid\":\"" + vmsLastTid + "\""
                    + "}";
        }
    }
}
