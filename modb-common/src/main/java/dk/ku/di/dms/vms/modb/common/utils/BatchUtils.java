package dk.ku.di.dms.vms.modb.common.utils;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.BATCH_OF_EVENTS;

public final class BatchUtils {

    private static final int JUMP = (2 * Integer.BYTES);

    public static int assembleBatchPayload(int remaining, List<TransactionEvent.PayloadRaw> events, ByteBuffer writeBuffer){
        int remainingBytes = writeBuffer.remaining();

        writeBuffer.put(BATCH_OF_EVENTS);
        // jump 2 integers
        writeBuffer.position(1 + JUMP);
        remainingBytes = remainingBytes - 1 - JUMP;

        // batch them all in the buffer,
        // until buffer capacity is reached or elements are all sent
        int count = 0;
        int idx = events.size() - remaining;
        while(idx < events.size() && remainingBytes > events.get(idx).totalSize()){
            TransactionEvent.writeWithinBatch( writeBuffer, events.get(idx) );
            remainingBytes = remainingBytes - events.get(idx).totalSize();
            idx++;
            count++;
        }

        int position = writeBuffer.position();
        writeBuffer.putInt(1, position);
        writeBuffer.putInt(5, count);
        writeBuffer.position(position);

        return remaining - count;
    }

    public static List<TransactionEvent.Payload> disAssembleBatchPayload(ByteBuffer byteBuffer) throws IOException
    {
//        System.out.println(STR."disAssembleBatchPayload");
        byteBuffer.position(0);
        var events = new ArrayList<TransactionEvent.Payload>();
        byte type = byteBuffer.get();
        if (type != BATCH_OF_EVENTS) throw new IOException("Invalid type");

        int segmentSize = byteBuffer.getInt();
        int eventCount = byteBuffer.getInt();

//            System.out.println(STR."ThesisLogger BATCH_OF_EVENTS event count: \{eventCount}");

        for (int i = 0; i < eventCount; i++) {
            var event = TransactionEvent.read(byteBuffer);
            events.add(event);
        }
        return events;
    }

}
