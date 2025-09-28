package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.function.Function;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;

public class ThesisLoggingHandlerV0 implements ILoggingHandler {

    protected final FileChannel fileChannel;
    protected final String fileName;

    public ThesisLoggingHandlerV0(FileChannel channel, String fileName) {
        this.fileChannel = channel;
        this.fileName = fileName;
    }

    @Override
    public final void close(){
        try {
            this.fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        System.out.println("modb.common.logging.ThesisLoggingHandlerV0.close");
    }
    @Override
    public TransactionEvent.Payload removeFailedEvent(ByteBuffer placeHolderBuffer, long failedTid, long batch) throws IOException
    {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        placeHolderBuffer.clear();

        long batchPosition = 0;
        fileChannel.position(batchPosition);

        TransactionEvent.Payload failedEvent = null;

        while (fileChannel.position() < fileChannel.size()) {
            metadataBuffer.clear();

            while (metadataBuffer.hasRemaining()) {
                int mn = fileChannel.read(metadataBuffer);
                if (mn == -1) {
                    throw new IOException("File too small");
                }
            }
            metadataBuffer.flip();

            byte type = metadataBuffer.get();
            if (type != BATCH_OF_EVENTS) throw new IOException("expected BATCH_OF_EVENTS, got " + type);

            int segmentSize = metadataBuffer.getInt();
            int count = metadataBuffer.getInt();
            long tid = metadataBuffer.getLong();
            long bid = metadataBuffer.getLong();

            // look at events
            if (bid == batch) {
                fileChannel.position(batchPosition);
                placeHolderBuffer.clear();
                placeHolderBuffer.limit(segmentSize);
                while (placeHolderBuffer.hasRemaining()) {
                    fileChannel.read(placeHolderBuffer);
                }
                placeHolderBuffer.flip();
                var events = BatchUtils.disAssembleBatchPayload(placeHolderBuffer);
                for (var event : events) {
                    if (event.tid() == failedTid) {
//                        System.out.println(STR."FAILED EVENT IS \{event}");
                        failedEvent = event;
                        break;
                    }
                }
                if (failedEvent != null) // shift whole file
                {
                    var bytesToShift = segmentSize;

                    // update current batch
                    var filteredEvents = events.stream().filter(e -> e.tid() != failedTid)
                            .map(e -> TransactionEvent.of(e.tid(), e.batch(), e.event(), e.payload(), e.precedenceMap()))
                            .toList();
                    var remainingEvents = filteredEvents.size();

                    // there are no more events
                    if (remainingEvents == 0)
                    {
                        //
                    }
                    placeHolderBuffer.clear();
                    if (remainingEvents > 0) {
                        while (remainingEvents > 0) {
                            remainingEvents = BatchUtils.assembleBatchPayload(remainingEvents, filteredEvents, placeHolderBuffer);
                        }

                        // write to file
                        placeHolderBuffer.flip();
                        var newSegmentSize = placeHolderBuffer.getInt(1);
                        placeHolderBuffer.limit(newSegmentSize);
                        fileChannel.position(batchPosition);
                        while (placeHolderBuffer.hasRemaining()) {
                            fileChannel.write(placeHolderBuffer);
                        }
                        bytesToShift -= newSegmentSize;
                    }
//                    System.out.println(STR."Must shift rest of file by \{bytesToShift} bytes, "+
//                                       STR."because oldsegsize=\{segmentSize} with " +
//                                       STR."remainingEvents=\{remainingEvents} and count=\{count}");

                    // Advance to next batch
                    batchPosition += segmentSize;
                    long readPosition = batchPosition;
                    long writePosition = batchPosition - bytesToShift;
                    placeHolderBuffer.limit(placeHolderBuffer.capacity());

//                    System.out.println(STR."Prior readPosition=\{batchPosition-segmentSize}, new readPosition=\{readPosition}, " +
//                                       STR."prior writePosition=\{batchPosition-segmentSize}, new ritePosition=\{writePosition}");

                    // Shift remaining file content
                    while (readPosition < fileChannel.size()) {
                        System.out.println(STR."readPosition=\{readPosition}");
                        placeHolderBuffer.clear();

                        int read = fileChannel.read(placeHolderBuffer, readPosition);
                        if (read == -1) break;

                        placeHolderBuffer.flip();
                        fileChannel.write(placeHolderBuffer, writePosition);

                        readPosition += read;
                        writePosition += read;
                    }
                    fileChannel.truncate(writePosition);
                    break;
                }
                else // advance to next part
                {
                    batchPosition += segmentSize;
                    fileChannel.position(batchPosition);
                }
            }
            else
            {
                batchPosition += segmentSize;
                fileChannel.position(batchPosition);
            }
        }
        return failedEvent;
    }

    // used in coordinator vms workers
    @Override
    public void fixPrecedence(ByteBuffer placeHolderBuffer,
                              long failedTid, long batch,
                              Map<String, Integer> precedenceMapOfFailedEvent,
                              Function<String, Map<String, Integer>> deserializer,
                              Function<Map<String, Integer>, String> serializer) throws IOException
    {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        placeHolderBuffer.clear();
        if (fileChannel.size() == 0) return;

        long batchPosition = 0;
        fileChannel.position(batchPosition);

        // all keys in precedenceMapOfFailedEvent set to false
        Set<String> precedenceToUpdate = new HashSet<>(precedenceMapOfFailedEvent.keySet());

        // go through log
        while (fileChannel.position() < fileChannel.size())
        {
            readMetadata(metadataBuffer);
            byte type = metadataBuffer.get();
            if (type != BATCH_OF_EVENTS) throw new IOException("expected BATCH_OF_EVENTS, got " + type);

            int segmentSize = metadataBuffer.getInt();
            int count = metadataBuffer.getInt();
            long tid = metadataBuffer.getLong();
            long bid = metadataBuffer.getLong();

            fileChannel.position(batchPosition);
            placeHolderBuffer.clear();
            placeHolderBuffer.limit(segmentSize);
            while (placeHolderBuffer.hasRemaining()) {
                fileChannel.read(placeHolderBuffer);
            }
            placeHolderBuffer.flip();
            var events = BatchUtils.disAssembleBatchPayload(placeHolderBuffer);
            var batchUpdated = false;
            for (int i = 0; i < events.size(); i++) {
                var event = events.get(i);
                var precedenceMap = deserializer.apply(event.precedenceMap());
                var updated = false;
                for (var precedence : new HashSet<>(precedenceToUpdate)) { // iterate over keys still to update
                    if (!precedenceMap.containsKey(precedence)) continue;
                    if (precedenceMap.get(precedence) != failedTid) continue;

                    var newPrecedenceTid = precedenceMapOfFailedEvent.get(precedence);
                    precedenceMap.put(precedence, newPrecedenceTid);
                    precedenceToUpdate.remove(precedence);

                    System.out.println(STR."Update precedence=\{precedence} for event=\{event.tid()} to precedenceId=\{newPrecedenceTid}");
                    updated = true;
                }
                if (updated) {
                    batchUpdated = true;
                    var updatedEvent = new TransactionEvent.Payload(
                            event.tid(),
                            event.batch(),
                            event.event(),
                            event.payload(),
                            serializer.apply(precedenceMap)
                    );
                    events.set(i, updatedEvent);
                }
            }
            if (batchUpdated)
            {
                var rawEvents = events.stream()
                        .map(e->TransactionEvent.of(e.tid(), e.batch(), e.event(), e.payload(), e.precedenceMap()))
                        .toList();

                var remainingEvents = rawEvents.size();
                placeHolderBuffer.clear();
                while (remainingEvents > 0)
                {
                    remainingEvents = BatchUtils.assembleBatchPayload(remainingEvents, rawEvents, placeHolderBuffer);
                }

                placeHolderBuffer.flip();
                placeHolderBuffer.limit(segmentSize);
                fileChannel.position(batchPosition);
                while (placeHolderBuffer.hasRemaining()) {
                    fileChannel.write(placeHolderBuffer);
                }
            }
            if (precedenceToUpdate.isEmpty()) return;

            batchPosition += segmentSize;
            fileChannel.position(batchPosition);
        }
    }

    private void readMetadata(ByteBuffer metadataBuffer) throws IOException
    {
        metadataBuffer.clear();
        while (metadataBuffer.hasRemaining()) {
            int mn = fileChannel.read(metadataBuffer);
            if (mn == -1) {
                throw new IOException("File too small");
            }
        }
        metadataBuffer.flip();
    }

    // use it in the VMSes. It truncates the file once it finds the failed event.
    // It assumes that batches may have been logged scattered and in between each other
    // thus traversing and updating the entire file is necessary
    @Override
    public int cutLog(ByteBuffer placeHolderBuffer, long failedTid, long batch) throws IOException
    {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        placeHolderBuffer.clear();

        if (fileChannel.size() == 0) return -1;
        int eventsInFailedBatch = 0;

        long batchPosition = 0;
        long writePosition = batchPosition;
        fileChannel.position(batchPosition);
//        System.out.println(STR."fileChannel.position()=\{fileChannel.position()} < \{fileChannel.size()}=fileChannel.size()");

        while (fileChannel.position() < fileChannel.size())
        {
            readMetadata(metadataBuffer);
            byte type = metadataBuffer.get();
            if (type != BATCH_OF_EVENTS) throw new IOException("expected BATCH_OF_EVENTS, got " + type);

            int segmentSize = metadataBuffer.getInt();
            int count = metadataBuffer.getInt();
            long tid = metadataBuffer.getLong();
            long bid = metadataBuffer.getLong();

            if (bid < batch)
            {
                // write this batch to an earlier position
                if (writePosition < batchPosition)
                {
                    // read batch into buffer
                    placeHolderBuffer.clear();
                    fileChannel.position(batchPosition);
                    placeHolderBuffer.limit(segmentSize);
                    while (placeHolderBuffer.hasRemaining()) {
                        fileChannel.read(placeHolderBuffer);
                    }
                    // write batch to earlier position
                    placeHolderBuffer.flip();
                    while (placeHolderBuffer.hasRemaining()) {
                        fileChannel.write(placeHolderBuffer, writePosition);
                    }
                    // jump two next events for reading
                    batchPosition += segmentSize;
                    writePosition += segmentSize;
                }
                else // just jump forward
                {
                    writePosition += segmentSize;
                    batchPosition += segmentSize;
                }
            }
            else if (bid > batch) // don't write batch
            {
                batchPosition += segmentSize; // jump to next segment
            }
            else // relevant
            {
                // read batch into buffer
                fileChannel.position(batchPosition);
                placeHolderBuffer.clear();
                placeHolderBuffer.limit(segmentSize);
                while (placeHolderBuffer.hasRemaining()) {
                    fileChannel.read(placeHolderBuffer);
                }

                // get events and filter
                placeHolderBuffer.flip();
                var events = BatchUtils.disAssembleBatchPayload(placeHolderBuffer);
                for (var event : events) {
                    System.out.println((event.tid() < failedTid ? "KEEP " : "DONT keep ") +
                                       STR."e.tid()=\{event.tid()} ?= failedTid=\{failedTid}");
                }

                var filteredEvents = events.stream()
                        .filter(e -> e.tid() < failedTid)
                        .map(e -> TransactionEvent.of(e.tid(), e.batch(), e.event(), e.payload(), e.precedenceMap()))
                        .toList();
                var remainingEvents = filteredEvents.size();
                eventsInFailedBatch += remainingEvents;

                if (remainingEvents == 0) // dont log anything
                {
                    batchPosition += segmentSize;
                }
                else if (remainingEvents == count) // no changes
                {
                    placeHolderBuffer.limit(segmentSize);
                    while (placeHolderBuffer.hasRemaining()) {
                        fileChannel.write(placeHolderBuffer, writePosition);
                    }
                    batchPosition += segmentSize;
                    writePosition += segmentSize;
                }
                else // log the events that are remaining
                {
                    placeHolderBuffer.clear();
                    while (remainingEvents > 0) {
                        remainingEvents = BatchUtils.assembleBatchPayload(remainingEvents, filteredEvents, placeHolderBuffer);
                    }

                    // write buffer into file
                    var newSegmentSize = placeHolderBuffer.getInt(1);
                    placeHolderBuffer.limit(newSegmentSize);
                    while (placeHolderBuffer.hasRemaining()) {
                        fileChannel.write(placeHolderBuffer);
                    }
                    batchPosition += segmentSize;
                    writePosition += newSegmentSize;
                }
            }
            fileChannel.position(batchPosition);
        }
        System.out.println(STR."File was truncated from \{fileChannel.size()} bytes to \{writePosition} bytes");
        fileChannel.truncate(writePosition);
        return eventsInFailedBatch;
    }

    @Override
    // scan through logs and find the batch (batch may have logged in batches, need to look through whole log)
    public int readBatch(ByteBuffer byteBuffer, long batch) throws IOException {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        byteBuffer.clear();

        int eventsInFailedBatch = 0;

        if (fileChannel.size() == 0) return eventsInFailedBatch;
        fileChannel.position(0);

        while (fileChannel.position() < fileChannel.size()) {
            metadataBuffer.clear();
            long batchPosition = fileChannel.position();

            while (metadataBuffer.hasRemaining()) {
                int mn = fileChannel.read(metadataBuffer);
                if (mn == -1) {
                    throw new IOException("File too small");
                }
            }
            metadataBuffer.flip();

            byte type = metadataBuffer.get();
            if (type != BATCH_OF_EVENTS) throw new IOException("expected BATCH_OF_EVENTS, got " + type);

            int segmentSize = metadataBuffer.getInt();
            int count = metadataBuffer.getInt();
            long tid = metadataBuffer.getLong();
            long bid = metadataBuffer.getLong();

            if (bid != batch) {
                System.out.println(STR."Batch is NOT bid in logs batch=\{batch} != \{bid}=bid");
                batchPosition += segmentSize;
                fileChannel.position(batchPosition);
                continue;
            }
            System.out.println(STR."Batch IS bid in logs batch=\{batch} == \{bid}=bid, Get \{count} events");

            if (byteBuffer.position() == 0)
            {
                // first time seeing a BATCH_OF_EVENTS
//                System.out.println("First time seeing BATCH_OF_EVENTS in log");
                byteBuffer.limit(segmentSize);
                fileChannel.position(batchPosition);
            }
            else
            {
                var metadataBytes = (1 + 2*Integer.BYTES);
                // update the count
                int oldCount = byteBuffer.getInt(5);
                int newCount = oldCount + count;
                int deltaSegmentSize = segmentSize-metadataBytes; // minus type, segmentSize, count
                int newSegmentSize = segmentSize + deltaSegmentSize;
                byteBuffer.putInt(1, newSegmentSize);
                byteBuffer.putInt(5, newCount);

//                System.out.println(STR."Append new BATCH_OF_EVENTS in log to existing batch, old count=\{oldCount} new count=\{newCount}");

                // set up the limits
                var oldLimit = byteBuffer.limit();
                var newLimit = oldLimit + deltaSegmentSize; // just add the events
                byteBuffer.limit(newLimit);
                byteBuffer.position(oldLimit);

                fileChannel.position(batchPosition + metadataBytes);
            }

            // read the events
            while (byteBuffer.hasRemaining()) {
                int n = fileChannel.read(byteBuffer);
                if (n == -1) throw new IOException("end of file");
            }
        }
        System.out.println(STR."Done reading batch \{batch}");
        eventsInFailedBatch = byteBuffer.getInt(5);
        return eventsInFailedBatch;
    }

    @Override
    public void log(ByteBuffer byteBuffer) throws IOException {
        System.out.println("logging");
        do {
            this.fileChannel.write(byteBuffer);
        } while(byteBuffer.hasRemaining());
    }

    @Override
    public SegmentMetadata loadSegment(ByteBuffer byteBuffer, long filePosition) throws IOException
    {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        fileChannel.position(filePosition);

        // read metadata
        System.out.println("Reading metadata");
        readMetadata(metadataBuffer);
        var segmentSize = metadataBuffer.getInt(1);
        var bid = metadataBuffer.getLong(17);

        System.out.println(STR."Read metadata bid=\{bid}");

        byteBuffer.clear();
        byteBuffer.limit(segmentSize);
        fileChannel.position(filePosition);
        while (byteBuffer.hasRemaining()) {
            fileChannel.read(byteBuffer);
        }

        var segmentMetadata = new SegmentMetadata(
                fileChannel.position() == fileChannel.size() ? -1 : fileChannel.position(),
                bid
        );

        return segmentMetadata;
    }

    @Override
    public final void force(){
        try {
            this.fileChannel.force(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        System.out.println("modb.common.logging.ThesisLoggingHandlerV0.force");
    }

    public final String getFileName() {
        return this.fileName;
    }
}