package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VmsReconnect
{
    public static void write(ByteBuffer buffer, String vms)
    {
        buffer.put(Constants.VMS_RECONNECTION);
        buffer.putInt( vms.length() );
        buffer.put( vms.getBytes(StandardCharsets.UTF_8) );
    }

    public static VmsReconnect.Payload read(ByteBuffer buffer){
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return VmsReconnect.of(vms);
    }

    public static VmsReconnect.Payload of(String vms) {
        return new VmsReconnect.Payload(vms);
    }

    public record Payload(
            String vms
    ) {
        @Override
        public String toString() {
            return "VmsReconnect {"
                    + "\"vms\":\"" + vms + "\""
                    + "}";
        }
    }
}
