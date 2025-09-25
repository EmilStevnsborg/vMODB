package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

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

    public class FailedEvent
    {
        public TransactionEvent.Payload event;
        public long batchPosition;
        public FailedEvent(TransactionEvent.Payload event, long batchPosition) {
            this.event = event;
            this.batchPosition = batchPosition;
        }
    }

    private FailedEvent findFailedEvent(ByteBuffer placeHolderBuffer, long failedTid, long batch) throws IOException
    {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        placeHolderBuffer.clear();

        long batchPosition = 0;
        long writePosition = batchPosition;
        fileChannel.position(batchPosition);
        System.out.println(STR."fileChannel.position()=\{fileChannel.position()} < \{fileChannel.size()}=fileChannel.size()");

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
                        System.out.println(STR."FAILED EVENT IS \{event}");
                        return new FailedEvent(event, batchPosition);
                    }
                }
            } else {
                batchPosition += segmentSize;
            }
            fileChannel.position(batchPosition);
        }
        return null;
    }

    // used in coordinator vms workers
    public void fixPrecedence(ByteBuffer placeHolderBuffer, long failedTid, long batch) throws IOException {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        placeHolderBuffer.clear();
        if (fileChannel.size() == 0) return;
        var failedEvent = findFailedEvent(placeHolderBuffer, failedTid, batch);
        if (failedEvent == null) return;

        // save for later
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
    public void cutLog(ByteBuffer placeHolderBuffer, long failedTid, long batch) throws IOException
    {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        placeHolderBuffer.clear();

        if (fileChannel.size() == 0) return;

        long batchPosition = 0;
        long writePosition = batchPosition;
        fileChannel.position(batchPosition);
        System.out.println(STR."fileChannel.position()=\{fileChannel.position()} < \{fileChannel.size()}=fileChannel.size()");

        while (fileChannel.position() < fileChannel.size())
        {
            readMetadata(metadataBuffer);
            byte type = metadataBuffer.get();
            if (type != BATCH_OF_EVENTS) throw new IOException("expected BATCH_OF_EVENTS, got " + type);

            int segmentSize = metadataBuffer.getInt();
            int count = metadataBuffer.getInt();
            long tid = metadataBuffer.getLong();
            long bid = metadataBuffer.getLong();

            System.out.println(STR."WritePosition=\{writePosition}");
            if (bid != batch) {
                System.out.println(STR."Batch is NOT bid in logs batch=\{batch} != \{bid}=bid at position=\{batchPosition}");

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
            else // correct batch
            {
                System.out.println(STR."Batch IS bid in logs batch=\{batch} == \{bid}=bid at position=\{batchPosition}");
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
                    System.out.println(STR."e.tid()=\{event.tid()} ?= failedTid=\{failedTid}");
                }

                var filteredEvents = events.stream()
                        .filter(e -> e.tid() < failedTid)
                        .map(e -> TransactionEvent.of(e.tid(), e.batch(), e.event(), e.payload(), e.precedenceMap()))
                        .toList();
                var remainingEvents = filteredEvents.size();

                for (var event : filteredEvents) {
                    System.out.println(STR."e.tid()=\{event.tid()} < failedTid=\{failedTid}");
                }

                if (remainingEvents == 0) // dont log anything
                {
                    System.out.println(STR."DONT log anything from bid=\{bid}");
                    batchPosition += segmentSize;
                }
                else if (remainingEvents == count) // no changes
                {
                    System.out.println(STR."LOG WHOLE batch");
                    placeHolderBuffer.limit(segmentSize);
                    while (placeHolderBuffer.hasRemaining()) {
                        fileChannel.write(placeHolderBuffer, writePosition);
                    }
                    batchPosition += segmentSize;
                    writePosition += segmentSize;
                }
                else // log the events that are remaining
                {
                    System.out.println(STR."LOG SOME of the batch");
                    placeHolderBuffer.clear();
                    BatchUtils.assembleBatchPayload(remainingEvents, filteredEvents, placeHolderBuffer);

                    // write buffer into file
                    var newSegmentSize = placeHolderBuffer.getInt(5);
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
    }

    @Override
    // scan through logs and find the batch (batch may have logged in batches, need to look through whole log)
    public int readBatch(ByteBuffer byteBuffer, long batch) throws IOException {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        byteBuffer.clear();

        if (fileChannel.size() == 0) return 0;

        fileChannel.position(0);
        System.out.println(STR."fileChannel.position()=\{fileChannel.position()} < \{fileChannel.size()}=fileChannel.size()");

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
            System.out.println(STR."Batch IS bid in logs batch=\{batch} == \{bid}=bid");
            System.out.println(STR."Get \{count} events");

            if (byteBuffer.position() == 0)
            {
                // first time seeing a BATCH_OF_EVENTS
                System.out.println("First time seeing BATCH_OF_EVENTS in log");
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

                System.out.println(STR."Append new BATCH_OF_EVENTS in log to existing batch, old count=\{oldCount} new count=\{newCount}");

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
            System.out.println(STR."Batch is over fileChannel.position()=\{fileChannel.position()}");
        }
        System.out.println(STR."Done reading batch \{batch}");
        return 1;
    }

    @Override
    public void log(ByteBuffer byteBuffer) throws IOException {
        System.out.println("logging");
        do {
            this.fileChannel.write(byteBuffer);
        } while(byteBuffer.hasRemaining());
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