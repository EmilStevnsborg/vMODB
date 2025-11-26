package dk.ku.di.dms.vms.modb.common.schema.network.batch;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * Payload that carries batch-commit information.
 * It may come appended with the batch of events
 * (as the last event) or may come alone.
 * Only sent to terminal nodes in a batch.
 */
public final class BatchCommitInfo {

    // message type + 3 longs + 1 int
    public static final int SIZE = 1 + (2 * Long.BYTES) + Integer.BYTES;

    public static void write(ByteBuffer buffer, long batch,
                             long previousBatch, int numberOfTIDsBatch,
                             Set<Long> abortedTIDs){
        buffer.put(Constants.BATCH_COMMIT_INFO);
        buffer.putLong( batch );
        buffer.putLong( previousBatch );
        buffer.putInt( numberOfTIDsBatch );
        buffer.putInt(abortedTIDs.size());
        for (var tid : abortedTIDs) {
            buffer.putLong(tid);
        }
    }

    public static void write(ByteBuffer buffer, Payload payload){
        buffer.put(Constants.BATCH_COMMIT_INFO);
        buffer.putLong( payload.batch );
        buffer.putLong(payload.previousBatch );
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

    public static Payload of(long batch, long previousBatch, int numberOfTIDsBatch, Set<Long> abortedTIDs){
        return new Payload(batch, previousBatch, numberOfTIDsBatch, abortedTIDs);
    }

    public record Payload(
            long batch, long previousBatch, int numberOfTIDsBatch, Set<Long> abortedTIDs
    ){
        @Override
        public String toString() {
            return "{"
                    + "\"batch\":\"" + batch + "\""
                    + ",\"previousBatch\":\"" + previousBatch + "\""
                    + ",\"numberOfTIDsBatch\":\"" + numberOfTIDsBatch + "\""
                    + ",\"numberOfTIDsAborted\":\"" + abortedTIDs.size() + "\""
                    + "}";
        }
    }

}
