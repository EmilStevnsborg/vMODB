package dk.ku.di.dms.vms.modb.common.schema.network.node;

import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class IdentifiableNode extends NetworkAddress {

    // identifier is the vms name
    public String identifier;

    // for JSON parsing
    public IdentifiableNode(){ }

    public IdentifiableNode(String identifier, String host, int port) {
        super(host, port);
        this.identifier = identifier;
    }

    @Override
    public String toString() {
        return "{" +
                "identifier='"+ identifier + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IdentifiableNode that)) return false;
        return this.identifier.contentEquals(that.identifier) && super.equals(that);
    }

    public void write(ByteBuffer buffer) {
        buffer.putInt(port);
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(hostBytes.length);
        buffer.put(hostBytes);
        byte[] nameBytes = identifier.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( nameBytes.length );
        buffer.put( nameBytes );
    }

    public static IdentifiableNode read(ByteBuffer buffer) {
        int port = buffer.getInt();
        int sizeHost = buffer.getInt();
        String host = ByteUtils.extractStringFromByteBuffer(buffer, sizeHost);
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return new IdentifiableNode(vms, host, port);
    }
}
