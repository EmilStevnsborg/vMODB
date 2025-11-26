package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Logger;

import static java.lang.System.Logger.Level.DEBUG;

public final class LongPairStore implements Closeable {
    private final String filename;
    private final FileChannel filechannel;
    private final ByteBuffer buffer;
    private long offset = 0;

    public LongPairStore(String filename, boolean truncate)
    {
        this.filename = filename;
        File file = buildFile(filename);
        try {
            StandardOpenOption[] options = buildFileOpenOptions(truncate);
            filechannel = FileChannel.open(Path.of(file.toURI()), options);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer = ByteBuffer.allocate(2 * Long.BYTES);
    }

    public void put(long batch, long maxTid) {
        try {
            buffer.clear();
            buffer.putLong(batch);
            buffer.putLong(maxTid);
            buffer.flip();
            while (buffer.hasRemaining()) {
                filechannel.write(buffer);
            }
            offset = filechannel.position();
            filechannel.force(false);
            System.out.println(STR."putting batch=\{batch}, tid=\{maxTid} into commit info \{filename}");
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
    public static StandardOpenOption[] buildFileOpenOptions(boolean truncate) {
        StandardOpenOption[] options;
//        System.out.println(STR."buildFileOpenOptions truncating=\{truncate}");
        if(truncate){
            options = new StandardOpenOption[]{
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.SPARSE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            };
        } else {
            options = new StandardOpenOption[]{
                    StandardOpenOption.CREATE,
                    StandardOpenOption.SPARSE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            };
        }
        return options;
    }
    public static String getBasePath(){
        String userHome = ConfigUtils.getUserHome();
        String basePath;
        if(ConfigUtils.isWindows()){
            basePath = userHome + "\\vms\\";
        } else {
            basePath = userHome + "/vms/";
        }
        return basePath;
    }
    public static File buildFile(String fileName) {
        String filePath = getBasePath() + fileName + ".data";
        File file = new File(filePath);
        return file;
    }

    @Override
    public void close() throws IOException {
        filechannel.force(true);
        filechannel.close();
    }
}
