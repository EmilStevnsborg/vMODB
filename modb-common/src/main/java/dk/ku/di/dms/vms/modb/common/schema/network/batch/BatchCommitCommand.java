package dk.ku.di.dms.vms.modb.common.schema.network.batch;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * A batch-commit request payload.
 * Sent to non-terminal nodes in a batch.
 * Sent after all terminal nodes have replied a BATCH_COMPLETE event
 */
public final class BatchCommitCommand {

    public static final int SIZE = 1 + (2 * Long.BYTES) + Integer.BYTES;

    public static void write(ByteBuffer buffer, BatchCommitCommand.Payload payload){
        buffer.put(Constants.BATCH_COMMIT_COMMAND);
        buffer.putLong(payload.batch);
        buffer.putLong(payload.previousBatch);
        buffer.putInt(payload.numberOfTIDsBatch);
        var abortedTIDs = payload.abortedTIDs;
        buffer.putInt(abortedTIDs.size());
        for (var tid : abortedTIDs) {
            buffer.putLong(tid);
        }
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        long previousBatch = buffer.getLong();
        int numberOfTIDsBatch = buffer.getInt();
        var abortedTIDs = new HashSet<Long>();
        var numTIDs = buffer.getInt();
        for (int i = 0; i < numTIDs; i++) {
            long tid = buffer.getLong();
            abortedTIDs.add(tid);
        }
        return new Payload(batch, previousBatch, numberOfTIDsBatch, abortedTIDs);
    }

    public record Payload(long batch, long previousBatch, int numberOfTIDsBatch, Set<Long> abortedTIDs){
        @Override
        public String toString() {
            return "{"
                    + "\"batch\":\"" + batch + "\""
                    + ",\"previousBatch\":\"" + previousBatch + "\""
                    + ",\"numberOfTIDsBatch\":\"" + numberOfTIDsBatch + "\""
                    + ",\"numberOfAbortedTIDsBatch\":\"" + abortedTIDs.size() + "\""
                    + "}";
        }
    }

}
