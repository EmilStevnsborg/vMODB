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
    protected final String fileName;
    private IVmsSerdesProxy serdesProxy;
    private final ConcurrentHashMap<Long, TransactionEvent.PayloadRaw> eventsSent = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Deque<ByteBuffer> writeBufferPool;
    private int writeBufferSize;

    public ThesisLoggingHandlerV1(FileChannel channel, String fileName, IVmsSerdesProxy serdesProxy, int bufferSize)
    {
        this.fileChannel = channel;
        this.fileName = fileName;
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
            System.out.println(STR."Logging event for tid=\{event.tid()} and bid=\{event.batch()}");
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

            if (committedEvents.isEmpty()) return true;

            try {
                BatchUtils.assembleBatchPayload(committedEvents.size(), committedEvents, buffer);
                buffer.limit(buffer.getInt(1));
                buffer.flip();

                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }
                fileChannel.force(false);

                // remove committed events atomically
                committedEvents.forEach(e -> eventsSent.remove(e.tid()));

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
    public List<TransactionEvent.PayloadRaw> getUncommittedEvents(List<String> eventTypes)
    {
        rwLock.readLock().lock();
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
    public long[] latestCommit() throws IOException
    {
        var buffer = retrieveByteBuffer();
        long latestCommittedBid = 0;
        long latestCommittedTid = 0;

        var segmentMetadataSize = 1 + 2 * Integer.BYTES + 2 * Long.BYTES;
        var metadataBuffer = ByteBuffer.allocate(segmentMetadataSize);

        long batchPosition = 0;
        fileChannel.position(batchPosition);

        while (fileChannel.position() < fileChannel.size())
        {
            byte type = metadataBuffer.get();
            if (type != BATCH_OF_EVENTS) throw new IOException("expected BATCH_OF_EVENTS, got " + type);

            int segmentSize = metadataBuffer.getInt();
            int count = metadataBuffer.getInt();
            long tid = metadataBuffer.getLong();
            long bid = metadataBuffer.getLong();

            if (bid < latestCommittedBid) {
                batchPosition += segmentSize;
                fileChannel.position(batchPosition);
                continue;
            }

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

            if (batchMaxTid > latestCommittedTid) latestCommittedBid = batchMaxTid;

            batchPosition += segmentSize;
            fileChannel.position(batchPosition);
        }

        return new long[]{latestCommittedBid, latestCommittedTid};
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
    public void abort(long failedTid, long failedTidBatch) {
        // quick existence check under read-lock for better concurrency
        rwLock.writeLock().lock();
        try {
            System.out.println(STR."Removing failed event: \{failedTid}");
            TransactionEvent.PayloadRaw failedEvent = findAndRemoveFailedEvent(failedTid);
            if (failedEvent == null) {
                System.out.println(STR."Can't find failed event in batch");
                return;
            }
            System.out.println(STR."Removing failed event Success: \{failedEvent.tid()}");

            System.out.println("Fixing precedence");
            fixPrecedence(failedEvent);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void cutLog(long failedTid, long failedTidBatch) {
        rwLock.writeLock().lock();
        try {
            System.out.println("Cutting log");
            eventsSent.values().stream().filter(e -> e.tid() < failedTid).forEach(e -> eventsSent.remove(e.tid()));
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
        return this.fileName;
    }
}
