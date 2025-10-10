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
    public static void write(ByteBuffer buffer, Recovery.Payload payload) {
        buffer.put(Constants.RECOVERY);
        buffer.putInt( payload.port );
        buffer.putInt( payload.host().length() );
        buffer.put( payload.host().getBytes(StandardCharsets.UTF_8) );
        buffer.putInt( payload.vms().length() );
        buffer.put( payload.vms().getBytes(StandardCharsets.UTF_8) );
    }

    public static void write(ByteBuffer buffer, IdentifiableNode identifiableNode) {
        buffer.put(Constants.RECOVERY);
        buffer.putInt(identifiableNode.port);
        byte[] hostBytes = identifiableNode.host.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(hostBytes.length);
        buffer.put(hostBytes);
        byte[] nameBytes = identifiableNode.identifier.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( nameBytes.length );
        buffer.put( nameBytes );
    }

    public static IdentifiableNode read(ByteBuffer buffer){
        int port = buffer.getInt();
        int sizeHost = buffer.getInt();
        String host = ByteUtils.extractStringFromByteBuffer(buffer, sizeHost);
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return new IdentifiableNode(vms, host, port);
    }

    public static Recovery.Payload of(IdentifiableNode identifiableNode) {
        return new Recovery.Payload(identifiableNode.identifier, identifiableNode.host, identifiableNode.port);
    }

    public record Payload(
            String vms, String host, int port
            ) {
        @Override
        public String toString() {
            return "Recovery {"
                    + "\"port\":\"" + port + "\","
                    + "\"host\":\"" + host + "\","
                    + "\"vms\":\"" + vms + "\""
                    + "}";
        }
    }
}
