package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;

public class ResetToCommittedState
{
    public static void write(ByteBuffer buffer, long newGeneration)
    {
        buffer.put(Constants.RESET_TO_COMMITTED);
        buffer.putLong(newGeneration);
    }

    public static Payload read(ByteBuffer buffer)
    {
        var newGeneration = buffer.getLong();
        return ResetToCommittedState.of(newGeneration);
    }

    public static Payload of(long newGeneration) {
        return new Payload(newGeneration);
    }

    public record Payload( long newGeneration
    ) {
        @Override
        public String toString() {
            return "ResetToCommittedState {"
                    + "\"newGeneration\":\"" + newGeneration + "\""
                    + "}";
        }
    }
}
