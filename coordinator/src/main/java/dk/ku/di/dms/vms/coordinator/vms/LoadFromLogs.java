package dk.ku.di.dms.vms.coordinator.vms;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class LoadFromLogs
{
    private final ILoggingHandler loggingHandler;
    public LoadFromLogs(ILoggingHandler loggingHandler)
    {
        this.loggingHandler = loggingHandler;
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
