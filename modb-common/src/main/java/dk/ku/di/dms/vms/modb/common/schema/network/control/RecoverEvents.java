package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;

public final class RecoverEvents
{

    public static RecoverEvents.Payload of(String vms, String host, int port) {
        return new RecoverEvents.Payload(vms, host, port);
    }

    public static IdentifiableNode crashedVms(RecoverEvents.Payload payload) {
        return new IdentifiableNode(payload.vms, payload.host, payload.port);
    }
    public record Payload(
            String vms, String host, int port
    ) {
        @Override
        public String toString() {
            return "RecoverEvents {"
                    + "\"port\":\"" + port + "\","
                    + "\"host\":\"" + host + "\","
                    + "\"vms\":\"" + vms + "\""
                    + "}";
        }
    }
}
