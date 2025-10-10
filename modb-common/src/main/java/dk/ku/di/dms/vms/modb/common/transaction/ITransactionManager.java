package dk.ku.di.dms.vms.modb.common.transaction;

import java.util.ArrayList;

/**
 * Interface to which client classes (i.e., event handler) can request a checkpoint of the state
 */
public interface ITransactionManager {

    default void checkpoint(long batch, long maxTid) { }
    default long[] latestCommitInfo() { return new long[] {0,0}; }

    // for abort in memory
    default void restoreStableState(long failedTid) { }

    default void commit() { }

    default ITransactionContext beginTransaction(long tid, int identifier, long lastTid, boolean readOnly) { return null; }

    default void reset() { }

}
