package dk.ku.di.dms.vms.modb.utils;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class LongPairStore implements Closeable {
    private final FileChannel filechannel;
    private final ByteBuffer buffer;
    private long offset = 0;

    public LongPairStore(String filename, boolean truncate)
    {
        StandardOpenOption[] options = StorageUtils.buildFileOpenOptions(truncate);
        var file = StorageUtils.buildFile(filename);
        try {
            filechannel = FileChannel.open(Path.of(file.toURI()), options);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer = ByteBuffer.allocate(2 * Long.BYTES);
    }

    public void put(long batch, long maxTid) {
        try {
            buffer.putLong(batch);
            buffer.putLong(maxTid);
            buffer.flip();
            while (buffer.hasRemaining()) {
                filechannel.write(buffer);
            }
            offset = filechannel.position();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long[] get(long offset) {
        try {
            buffer.position(0);
            while (buffer.hasRemaining()) {
                filechannel.read(buffer, offset);
            }
            buffer.flip();
            return new long[]{buffer.getLong(), buffer.getLong()};
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long[] getLatest() {
        try {
            var size = filechannel.size();
            if (size < 2*Long.BYTES) {
                // default
                return new long[] {0,0};
            }
            buffer.position(0);
            while (buffer.hasRemaining()) {
                filechannel.read(buffer, size-2*Long.BYTES);
            }
            buffer.flip();
            return new long[]{buffer.getLong(), buffer.getLong()};
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        filechannel.force(true);
        filechannel.close();
    }
}
