package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.nio.ByteBuffer;

public interface IVmsContainer {
    void queue(TransactionEvent.PayloadRaw payload);
    int cutLog(long failedTid, long batch);

    // consumer worker for recovered VMS
    void recover(long latestCommittedBatch);

    // consumer worker sends events to recovered consumer
    void processRecoveryInVms();
    String identifier();

    default void stop() { }
}