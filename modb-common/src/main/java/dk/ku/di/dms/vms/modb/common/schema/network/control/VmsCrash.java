package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VmsCrash
{
    public static void write(ByteBuffer buffer, String vms, long newGeneration)
    {
        buffer.put(Constants.VMS_CRASH);
        buffer.putInt( vms.length() );
        buffer.put( vms.getBytes(StandardCharsets.UTF_8) );
        buffer.putLong( newGeneration );
    }

    public static Payload read(ByteBuffer buffer){
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        long newGeneration = buffer.getLong();
        return VmsCrash.of(vms, newGeneration);
    }

    public static Payload of(String vms, long newGeneration) {
        return new Payload(vms, newGeneration);
    }

    public record Payload(
            String vms,
            long newGeneration
    ) {
        @Override
        public String toString() {
            return "VmsCrash {"
                    + "\"vms\":\"" + vms + "\""
                    + ",\"newGeneration\":\"" + newGeneration + "\""
                    + "}";
        }
    }
}
