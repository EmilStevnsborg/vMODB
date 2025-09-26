package dk.ku.di.dms.vms.coordinator.vms;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class FromLogs
{
    private final ILoggingHandler loggingHandler;
    private final IVmsSerdesProxy serdesProxy;
    public FromLogs(ILoggingHandler loggingHandler, IVmsSerdesProxy serdesProxy)
    {
        this.loggingHandler = loggingHandler;
        this.serdesProxy = serdesProxy;
    }

    // remove failedEvent and update log
    public TransactionEvent.Payload removeFailedEvent(ByteBuffer byteBuffer, long failedTid, long batch)
    {
        TransactionEvent.Payload failedEvent = null;
        try {
            failedEvent = loggingHandler.removeFailedEvent(byteBuffer, failedTid, batch);
        } catch (Exception e) {
            System.out.println(STR."removing failed event caused an error: \{e}");
            e.printStackTrace();
        }
        if (failedEvent == null) {
            return null;
        }
        return failedEvent;
    }

    // fix precedence and update log
    public void fixPrecedence(ByteBuffer byteBuffer, TransactionEvent.Payload failedEvent)
    {
        try
        {
            Function<String, Map<String, Integer>> deserializer = s -> serdesProxy.<String, Integer>deserializeMap(s);
            Function<Map<String, Integer>, String> serializer = m -> serdesProxy.serialize(m, Map.class);
            var failedEventPrecedenceMap = deserializer.apply(failedEvent.precedenceMap());

            loggingHandler.fixPrecedence(
                    byteBuffer,
                    failedEvent.tid(), failedEvent.batch(),
                    failedEventPrecedenceMap,
                    deserializer, serializer
            );
            this.readBatch(byteBuffer, failedEvent.batch());
        } catch (Exception e)
        {
            System.out.println("fixPrecedence failed");
            e.printStackTrace();
        }
    }

    private void readBatch(ByteBuffer byteBuffer, long batch)
    {
        System.out.println("readBatch");
        List<TransactionEvent.Payload> eventsInBatchInLogs = null;
        try {
            loggingHandler.readBatch(byteBuffer, batch);
            var eventsInBatchInLogsTmp = BatchUtils.disAssembleBatchPayload(byteBuffer);
            eventsInBatchInLogs = eventsInBatchInLogsTmp;
            System.out.println(STR."eventsInBatchInLogs size=\{eventsInBatchInLogs.size()}");
            for (var event : eventsInBatchInLogs) {
                System.out.println(STR."eventInBatchLogs: \{event}");
            }
        } catch (Exception e) {
            System.out.println("Couldn't read batch");
        }
    }
}
