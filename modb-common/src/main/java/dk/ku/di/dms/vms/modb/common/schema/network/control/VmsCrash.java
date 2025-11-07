package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VmsCrash
{
    public static void write(ByteBuffer buffer, String vms)
    {
        buffer.put(Constants.VMS_CRASH);
        buffer.putInt( vms.length() );
        buffer.put( vms.getBytes(StandardCharsets.UTF_8) );
    }

    public static VmsCrash.Payload read(ByteBuffer buffer){
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return VmsCrash.of(vms);
    }

    public static VmsCrash.Payload of(String vms) {
        return new VmsCrash.Payload(vms);
    }

    public record Payload(
            String vms
    ) {
        @Override
        public String toString() {
            return "VmsCrash {"
                    + "\"vms\":\"" + vms + "\""
                    + "}";
        }
    }
}
