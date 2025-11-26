package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;

public class ResetToCommittedState
{
    public static void write(ByteBuffer buffer)
    {
        buffer.put(Constants.RESET_TO_COMMITTED);
    }

    public static Payload read(ByteBuffer buffer){
        return ResetToCommittedState.of();
    }

    public static Payload of() {
        return new Payload();
    }

    public record Payload(
    ) {
        @Override
        public String toString() {
            return "ResetToCommittedState {"
                    + "}";
        }
    }
}
