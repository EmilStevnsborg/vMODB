package dk.ku.di.dms.vms.modb.common.schema.network.control;

public final class RecoverEvents
{

    public static RecoverEvents.Payload of(String vms, String host, int port) {
        return new RecoverEvents.Payload(vms, host, port);
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
