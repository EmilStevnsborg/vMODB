package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ResetToCommittedState
{
    public static void write(ByteBuffer buffer)
    {
        buffer.put(Constants.RESET_TO_COMMITTED);
    }

    public static ResetToCommittedState.Payload read(ByteBuffer buffer){
        return ResetToCommittedState.of();
    }

    public static ResetToCommittedState.Payload of() {
        return new ResetToCommittedState.Payload();
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
