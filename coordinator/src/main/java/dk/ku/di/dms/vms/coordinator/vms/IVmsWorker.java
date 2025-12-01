package dk.ku.di.dms.vms.coordinator.vms;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface IVmsWorker {
    default void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) { }
    default void requeueTransactionEvent(TransactionEvent.PayloadRaw payload) {}
    default void clear() {};

    default void queueMessage(Object message) { }

    default long getTid() { return 0; }

    default void stop() { }

}
