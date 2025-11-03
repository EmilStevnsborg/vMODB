package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbortInfo;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class Recovery
{
    public static final int SIZE = 1 + (2 * Long.BYTES);
    public static void write(ByteBuffer buffer, Recovery.Payload payload) {
        buffer.put(Constants.RECOVERY);
        buffer.putInt( payload.vms().length() );
        buffer.put( payload.vms().getBytes(StandardCharsets.UTF_8) );
    }

    public static void write(ByteBuffer buffer, IdentifiableNode identifiableNode) {
        buffer.put(Constants.RECOVERY);
        byte[] nameBytes = identifiableNode.identifier.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( nameBytes.length );
        buffer.put( nameBytes );
    }

    public static Recovery.Payload read(ByteBuffer buffer){
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return Recovery.of(vms);
    }

    public static Recovery.Payload of(String vms) {
        return new Recovery.Payload(vms);
    }

    public record Payload(
            String vms
            ) {
        @Override
        public String toString() {
            return "Recovery {"
                    + "\"vms\":\"" + vms + "\""
                    + "}";
        }
    }
}
