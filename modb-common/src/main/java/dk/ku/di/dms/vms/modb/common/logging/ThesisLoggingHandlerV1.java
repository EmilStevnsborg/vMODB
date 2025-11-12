package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.BATCH_OF_EVENTS;


//////////// CONCURRENT IN MEMORY ////////////


/// PUT LOCK ON MEMORY WHEN CALLING CERTAIN METHODS




public class ThesisLoggingHandlerV1 implements ILoggingHandler
{
    protected final FileChannel fileChannel;
    private IVmsSerdesProxy serdesProxy;
    private final HashMap<Long, TransactionEvent.PayloadRaw> eventsSent = new HashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Deque<ByteBuffer> writeBufferPool;
    private int writeBufferSize;

    public ThesisLoggingHandlerV1(FileChannel channel, IVmsSerdesProxy serdesProxy, int bufferSize)
    {
        this.fileChannel = channel;
        this.serdesProxy = serdesProxy;
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        writeBufferSize = bufferSize;
    }
    private ByteBuffer retrieveByteBuffer() {
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(writeBufferSize);
    }

    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        this.writeBufferPool.add(bb);
    }

    @Override
    public void log(TransactionEvent.PayloadRaw event)
    {
        rwLock.writeLock().lock();
        try {
            // System.out.println(STR."Logging event for tid=\{event.tid()} and bid=\{event.batch()}");
            eventsSent.put(event.tid(), event);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public boolean commit(long bid)
    {
        rwLock.readLock().lock();
        try {
            var buffer = retrieveByteBuffer();
            var committedEvents = eventsSent.values().stream()
                    .filter(e -> e.batch() == bid)
                    .toList();

            System.out.println(STR."Committing batch=\{bid} of \{committedEvents.size()} events");

            if (committedEvents.isEmpty()) return true;

            try {
                BatchUtils.assembleBatchPayload(committedEvents.size(), committedEvents, buffer);
                buffer.limit(buffer.getInt(1));
                buffer.flip();

                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }
                fileChannel.force(true);
                System.out.println(STR."LoggingHandler Committing batch=\{bid} has been forced");

                // remove committed events atomically
                committedEvents.forEach(e -> {
                    eventsSent.remove(e.tid());
                });

                returnByteBuffer(buffer);
                return true;
            } catch (IOException ex) {
                ex.printStackTrace();
                returnByteBuffer(buffer);
                return false;
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<TransactionEvent.PayloadRaw> getUncommittedEvents(long batch)
    {
        rwLock.readLock().lock();
        try
        {
            return eventsSent.values().stream().filter(e -> e.batch() == batch).toList();
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<TransactionEvent.PayloadRaw> getUncommittedEvents(List<String> eventTypes)
    {
        rwLock.readLock().lock();
        eventsSent.values().forEach(e -> {
        });
        try
        {
            return eventsSent.values().stream().filter(e-> {
                var eventString = new String(e.event(), StandardCharsets.UTF_8);
                return eventTypes.contains(eventString);
            }).toList();
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public SegmentMetadata loadSegment(ByteBuffer byteBuffer, long filePosition) throws IOException
    {
        ByteBuffer metadataBuffer = ByteBuffer.allocate(1 + 2 * Integer.BYTES + 2 * Long.BYTES); // 25 bytes
        fileChannel.position(filePosition);

        // read metadata
        readMetadata(metadataBuffer);
        var segmentSize = metadataBuffer.getInt(1);
        var eventCount = metadataBuffer.getInt(5);
        var bid = metadataBuffer.getLong(17);

        byteBuffer.clear();
        byteBuffer.limit(segmentSize);
        fileChannel.position(filePosition);
        while (byteBuffer.hasRemaining()) {
            fileChannel.read(byteBuffer);
        }

        var segmentMetadata = new SegmentMetadata(
                fileChannel.position() == fileChannel.size() ? -1 : fileChannel.position(),
                bid, eventCount
        );

        return segmentMetadata;
    }

    @Override
    public Map<String, long[]> getLatestAppearanceOfEventTypes(List<String> eventTypes) throws IOException
    {
        Map<String, long[]> latestAppearances = new HashMap<>();

        var buffer = retrieveByteBuffer();
        long batchPosition = 0;
        fileChannel.position(batchPosition);

//        System.out.println(STR."batchPosition=\{batchPosition}, fileChannel.position()=\{fileChannel.position()}, fileChannel.size()=\{fileChannel.size()}");
        while (batchPosition != -1 && fileChannel.position() < fileChannel.size())
        {
            fileChannel.position(batchPosition);
            var segmentMetadata = loadSegment(buffer, batchPosition);
//            System.out.println(STR."Read segment bid=\{segmentMetadata.bid} of event count \{segmentMetadata.eventCount}");
            buffer.flip();
            var events = BatchUtils.disAssembleBatchPayload(buffer);
            for (var event : events)
            {
                var type = event.event();
                long tid = event.tid();
                long batch = event.batch();

                if (!latestAppearances.containsKey(type) || latestAppearances.get(type)[0] < tid)
                {
//                    System.out.println(STR."New tid and bid for \{type} is \{tid} \{batch}");
                    latestAppearances.put(type, new long[]{tid, batch});
                }
            }
            buffer.clear();
            batchPosition = segmentMetadata.nextFilePosition;
        }
        returnByteBuffer(buffer);

        return latestAppearances;
    }

    @Override
    public long[] latestCommit() throws IOException
    {
        var buffer = retrieveByteBuffer();
        long latestCommittedBid = 0;
        long latestCommittedTid = 0;
        long numTIDs = 0;

        var segmentMetadataSize = 1 + 2 * Integer.BYTES + 2 * Long.BYTES;
        var metadataBuffer = ByteBuffer.allocate(segmentMetadataSize);

        long batchPosition = 0;
        fileChannel.position(batchPosition);

        System.out.println(STR."Loading latest commit from logs file size=\{fileChannel.size()}");

        while (fileChannel.position() < fileChannel.size())
        {
            metadataBuffer.clear();
            readMetadata(metadataBuffer);

            byte type = metadataBuffer.get();
            if (type != BATCH_OF_EVENTS) throw new IOException("expected BATCH_OF_EVENTS, got " + type);

            int segmentSize = metadataBuffer.getInt();
            int count = metadataBuffer.getInt();
            long tid = metadataBuffer.getLong();
            long bid = metadataBuffer.getLong();

            if (bid < latestCommittedBid) {
                System.out.println(STR."bid=\{bid} in file is less than \{latestCommittedBid}");
                batchPosition += segmentSize;
                fileChannel.position(batchPosition);
                continue;
            }

            numTIDs = count;
            latestCommittedBid = bid;

            fileChannel.position(batchPosition);
            buffer.limit(segmentSize);
            while (buffer.hasRemaining()) {
                fileChannel.read(buffer);
            }
            buffer.flip();
            var events = BatchUtils.disAssembleBatchPayload(buffer);
            var batchMaxTid = events.stream()
                    .mapToLong(TransactionEvent.Payload::tid)
                    .max()
                    .orElse(-1);

            if (batchMaxTid > latestCommittedTid) latestCommittedTid = batchMaxTid;

            System.out.println(STR."Updated: latestCommittedBid=\{latestCommittedBid}, latestCommittedTid=\{latestCommittedTid}");

            batchPosition += segmentSize;
            fileChannel.position(batchPosition);
        }

        return new long[]{latestCommittedBid, latestCommittedTid, numTIDs};
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

    // helper so lock is called before
    private TransactionEvent.PayloadRaw findAndRemoveFailedEvent(long failedTid) {
        var event = eventsSent.get(failedTid);
        eventsSent.remove(failedTid);
        return event;
    }

    // Helper, so lock is called before
    private void fixPrecedence(TransactionEvent.PayloadRaw failedEventRaw)
    {
        if (failedEventRaw == null) return;
        var failedEventReadable = TransactionEvent.read(failedEventRaw);

        Map<String, Integer> precedenceMapOfFailedEvent = serdesProxy.deserializeMap(failedEventReadable.precedenceMap());
        if (precedenceMapOfFailedEvent == null || precedenceMapOfFailedEvent.isEmpty()) return;

        HashSet<String> precedenceToUpdate = new HashSet<>(precedenceMapOfFailedEvent.keySet());

        if (eventsSent.isEmpty()) return;

        // iterate with index because we may replace element
        for (var entry : eventsSent.entrySet()) {
            var event = entry.getValue();
            if (event.tid() <= failedEventRaw.tid()) continue;


            var eventReadable = TransactionEvent.read(event);
            Map<String, Integer> eventPrecedenceMap = serdesProxy.deserializeMap(eventReadable.precedenceMap());
            boolean updated = false;

            // iterate over a copy to avoid ConcurrentModification on the set
            for (String precedence : new HashSet<>(precedenceToUpdate))
            {
                if (!eventPrecedenceMap.containsKey(precedence)) continue;
                if (eventPrecedenceMap.get(precedence) != failedEventRaw.tid()) continue;

                var newTid = precedenceMapOfFailedEvent.get(precedence);
                eventPrecedenceMap.put(precedence, newTid);
                System.out.println(STR."Setting precedence \{precedence} for \{eventReadable.tid()} to \{newTid}");
                precedenceToUpdate.remove(precedence);
                updated = true;
            }

            if (updated) {
                var updatedEventRaw = TransactionEvent.of(
                        eventReadable.tid(),
                        eventReadable.batch(),
                        eventReadable.event(),
                        eventReadable.payload(),
                        serdesProxy.serializeMap(eventPrecedenceMap)
                );
                // atomically replace the event in the map
                eventsSent.replace(entry.getKey(), updatedEventRaw);
            }
            if (precedenceToUpdate.isEmpty()) return;
        }
    }

    @Override
    public List<TransactionEvent.PayloadRaw> abort(List<Long> failedTIDs) {
        rwLock.writeLock().lock();
        List<TransactionEvent.PayloadRaw> abortedEvents = new ArrayList<>();

        try {
            for (long failedTid : failedTIDs) {
                TransactionEvent.PayloadRaw failedEvent = findAndRemoveFailedEvent(failedTid);
                if (failedEvent == null) {
                    System.out.println(STR."Can't find failed event in batch");
                    continue;
                }

                String eventName = new String(failedEvent.event(), StandardCharsets.UTF_8);
                System.out.println(STR."Removing \{eventName} with tid \{failedEvent.tid()} of batch \{failedEvent.batch()}");

                System.out.println("Fixing precedence");
                fixPrecedence(failedEvent);

                abortedEvents.add(failedEvent);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rwLock.writeLock().unlock();
        }

        return abortedEvents;
    }
    @Override
    public TransactionEvent.PayloadRaw abort(long failedTid) {
        // quick existence check under read-lock for better concurrency
        rwLock.writeLock().lock();
        try {
            System.out.println(STR."Removing failed event: \{failedTid}");
            TransactionEvent.PayloadRaw failedEvent = findAndRemoveFailedEvent(failedTid);
            if (failedEvent == null) {
                System.out.println(STR."Can't find failed event in batch");
                return null;
            }
            var eventName = new String(failedEvent.event(), StandardCharsets.UTF_8);
            System.out.println(STR."Removing \{eventName} with tid \{failedEvent.tid()} of batch \{failedEvent.batch()}");

            fixPrecedence(failedEvent);
            return failedEvent;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void cutLog(long failedTid) {
        rwLock.writeLock().lock();
        try {
            eventsSent.values().stream().filter(e -> e.tid() >= failedTid).forEach(e -> eventsSent.remove(e.tid()));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public List<TransactionEvent.PayloadRaw> getAffectedEvents(long failedTid) {
        rwLock.readLock().lock();
        try {
            return eventsSent.values().stream().filter(e -> e.tid() > failedTid).toList();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public int countEventsInBatch(long batch) {
        rwLock.readLock().lock();
        try {
            return (int) eventsSent.values().stream().filter(e -> e.batch() == batch).count();
        } finally {
            rwLock.readLock().unlock();
        }
    }


    @Override
    public final void close(){
        try {
            this.fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("modb.common.logging.DefaultLoggingHandler.close");
    }

    @Override
    public final void force(){
        try {
            this.fileChannel.force(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("modb.common.logging.DefaultLoggingHandler.force");
    }

    public final String getFileName() {
        return "";
    }
}
