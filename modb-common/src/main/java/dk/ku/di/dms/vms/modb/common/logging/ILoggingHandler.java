package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Interface for logging implementations
 */
public interface ILoggingHandler {

    default void log(ByteBuffer byteBuffer) throws IOException { }
    default int readBatch(ByteBuffer byteBuffer, long batch) throws IOException { return 0; }

    default void force() { }

    default void close() { }

    default String getFileName() { return ""; }
}
