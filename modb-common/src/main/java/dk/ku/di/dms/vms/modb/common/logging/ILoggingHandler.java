package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * Interface for logging implementations
 */
public interface ILoggingHandler {

    default void log(TransactionEvent.PayloadRaw payloadRaw) {}
    default boolean commit(long bid) { return false; }
    default long[] latestCommit() throws IOException { return new long[] {0,0,0}; }
    default TransactionEvent.PayloadRaw abort(long failedTid) { return null; }
    default List<TransactionEvent.PayloadRaw> abort(List<Long> failedTIDs) { return List.of(); }
    default void cutLog(long failedTid) {}
    default int countEventsInBatch(long batch) { return 0; }
    default List<TransactionEvent.PayloadRaw> getAffectedEvents(long failedTid) { return List.of(); }
    default Map<String, long[]> getLatestAppearanceOfEventTypes(List<String> eventTypes) throws IOException { return Map.of(); }

    default SegmentMetadata loadSegment(ByteBuffer byteBuffer, long filePosition) throws IOException { return null; }

    default void force() { }

    default void close() { }

    default String getFileName() { return ""; }
}
