package dk.ku.di.dms.vms.modb.common.logging;

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
//            this.readBatch(byteBuffer, failedEvent.batch());
        } catch (Exception e)
        {
            System.out.println("fixPrecedence failed");
            e.printStackTrace();
        }
    }

    // skip bid < failedTidBatch, send bid > failedTidBatch, filter bid == failedTidBatch by tid > failedTid
    public SegmentMetadata loadSegment(ByteBuffer byteBuffer, long filePosition, long failedTid, long failedTidBatch)
    {
        try {
            var segmentMetadata = loggingHandler.loadSegment(byteBuffer, filePosition);
            if (segmentMetadata.bid != failedTidBatch) return segmentMetadata;
            // deconstruct first segment
            byteBuffer.flip();
            var events = BatchUtils.disAssembleBatchPayload(byteBuffer);
            var filteredEvents = events
                    .stream().filter(e->e.tid()>failedTid)
                    .toList();
//            for (var filteredEvent : filteredEvents)
//            {
//                System.out.println(STR."First segment send: \{filteredEvent}");
//            }
            var filteredRawEvents = filteredEvents
                    .stream()
                    .map(e ->
                            TransactionEvent.of(
                                    e.tid(), e.batch(),
                                    e.event(), e.payload(),
                                    e.precedenceMap()))
                    .toList();;
            var remainingEvents = filteredRawEvents.size();
            if (remainingEvents == 0) {
                segmentMetadata.eventCount = 0;
                return segmentMetadata;
            }

            byteBuffer.clear();
            while (remainingEvents > 0) {
                remainingEvents = BatchUtils.assembleBatchPayload(remainingEvents, filteredRawEvents, byteBuffer);
            }
            byteBuffer.limit(byteBuffer.getInt(1));
            return segmentMetadata;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // load each segment later than last committed batch
    public SegmentMetadata loadSegment(ByteBuffer byteBuffer, long filePosition)
    {
        try
        {
            var segmentMetadata = loggingHandler.loadSegment(byteBuffer, filePosition);
            return segmentMetadata;
        } catch (Exception e) {
            throw new RuntimeException(e);
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
