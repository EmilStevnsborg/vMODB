package dk.ku.di.dms.vms.coordinator.vms;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class FromLogs
{
    private final ILoggingHandler loggingHandler;
    public FromLogs(ILoggingHandler loggingHandler)
    {
        this.loggingHandler = loggingHandler;
    }

    // fix precedence and update log
    public void fixPrecedence(ByteBuffer byteBuffer, long failedTid, long batch) throws InterruptedException
    {
        TransactionEvent.Payload failedEvent = null;
        try {
            failedEvent = loggingHandler.removeFailedEvent(byteBuffer, failedTid, batch);
        } catch (Exception e) {
            System.out.println(STR."removing failed event caused an error: \{e}");
            e.printStackTrace();
        }
        if (failedEvent == null) {
            return;
        }
        System.out.println(STR."Failed event: \{failedEvent}");
        List<TransactionEvent.Payload> eventsInBatchInLogs = null;
        try {
            loggingHandler.readBatch(byteBuffer, batch);
            var eventsInBatchInLogsTmp = BatchUtils.disAssembleBatchPayload(byteBuffer);
            eventsInBatchInLogs = eventsInBatchInLogsTmp;
        } catch (Exception e) {
            System.out.println("Couldn't read batch");
        }
        System.out.println(STR."eventsInBatchInLogs count=\{eventsInBatchInLogs.size()}");
        if (eventsInBatchInLogs == null) {
            System.out.println("eventsInBatchInLogs is null");
            return;
        }
        for (var event : eventsInBatchInLogs) {
            System.out.println(STR."eventInBatchLogs: \{event}");
        }
    }

    public int loadEventsToResend(ByteBuffer byteBuffer, long failedTid, long batch) throws IOException {
        // fill buffer from file
        var result = loggingHandler.readBatch(byteBuffer, batch);
        if (result != 1) return 0;

        // flip to read
        byteBuffer.flip();
        var events = BatchUtils.disAssembleBatchPayload(byteBuffer);
        System.out.println(STR."eventsSize=\{events.size()}");
        for (var event : events)
        {
            System.out.println(STR."event in batch:\n\{event}");
        }

        // fix precedence
        var failedEventPrecedenceMap = events.stream()
                .filter(e->e.tid() == failedTid)
                .findFirst().get()
                .precedenceMap(); // string
        // find events with precedence to failedTid and update it.

        events.replaceAll(e ->
                e.precedenceMap().equals(failedTid) // not valid condition
                        ? new TransactionEvent.Payload(e.tid(), e.batch(), e.event(), e.payload(), failedEventPrecedenceMap)
                        : e
        );

        var filteredEvents = events.stream()
                .filter(e -> e.tid() > failedTid)
                        .toList();

        for (var filteredEvent : filteredEvents)
        {
            System.out.println(STR."event that needs to be resend:\n\{filteredEvent}");
        }

        var filteredRawEvents = filteredEvents.stream()
                .map(e -> TransactionEvent.of(e.tid(), e.batch(), e.event(), e.payload(), e.precedenceMap()))
                .toList();

        // clear buffer to write new batch
        byteBuffer.clear();
        BatchUtils.assembleBatchPayload(filteredRawEvents.size(), filteredRawEvents, byteBuffer);

        // flip to read when writing to channel
        byteBuffer.flip();

        return 1;
    }
}
