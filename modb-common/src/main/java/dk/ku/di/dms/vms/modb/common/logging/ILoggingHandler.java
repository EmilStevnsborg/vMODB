package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;


/**
 * Interface for logging implementations
 */
public interface ILoggingHandler {

    default void log(ByteBuffer byteBuffer) throws IOException { }
    default SegmentMetadata loadSegment(ByteBuffer byteBuffer, long filePosition) throws IOException { return null; };
    default TransactionEvent.Payload removeFailedEvent(ByteBuffer placeHolderBuffer, long failedTid, long batch) throws IOException { return null; }
    default void fixPrecedence(ByteBuffer placeHolderBuffer,
                               long failedTid, long batch,
                               Map<String, Integer> precedenceMapOfFailedEvent,
                               Function<String, Map<String, Integer>> deserializer,
                               Function<Map<String, Integer>, String> serializer) throws IOException { }
    default int cutLog(ByteBuffer placeHolderBuffer, long failedTid, long batch) throws IOException { return -1;}
    default int readBatch(ByteBuffer byteBuffer, long batch) throws IOException { return 0; }

    default void force() { }

    default void close() { }

    default String getFileName() { return ""; }
}
