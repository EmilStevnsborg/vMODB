package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.nio.ByteBuffer;

public interface IVmsContainer {
    void queue(TransactionEvent.PayloadRaw payload);

    // consumer worker initiates reconnect to recovered consumer
    void processRecoveryInVms();
    String identifier();

    default void stop() { }
}