package dk.ku.di.dms.vms.modb.common.logging;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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