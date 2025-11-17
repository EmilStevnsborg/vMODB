package dk.ku.di.dms.vms.modb.storage.record;

import java.lang.foreign.MemorySegment;

/**
 * This class encapsulates a managed {@link MemorySegment}
 */
public sealed class RecordBufferContext permits AppendOnlyBuffer {

    // just to avoid many method calls, since this value does not change
    public final long address;

    protected final MemorySegment memorySegment;

    // if segment is a mapped file
    public final String fileName;

    public static RecordBufferContext build(MemorySegment memorySegment){
        return new RecordBufferContext(memorySegment, null);
    }

    public static RecordBufferContext build(MemorySegment memorySegment, String fileName){
        return new RecordBufferContext(memorySegment, fileName);
    }

    protected RecordBufferContext(MemorySegment memorySegment, String fileName) {
        this.memorySegment = memorySegment;
        this.address = this.memorySegment.address();
        this.fileName = fileName;
    }

    public final void force(){
        // System.out.println(STR."flushing memorySegment");
        this.memorySegment.force();
    }

}
