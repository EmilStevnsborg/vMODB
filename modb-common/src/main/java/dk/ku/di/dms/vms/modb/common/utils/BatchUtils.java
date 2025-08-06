package dk.ku.di.dms.vms.modb.common.utils;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.nio.ByteBuffer;
import java.util.List;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.BATCH_OF_EVENTS;

public final class BatchUtils {

    private static final int JUMP = (2 * Integer.BYTES);

    public static int assembleBatchPayload(int remaining, List<TransactionEvent.PayloadRaw> events, ByteBuffer writeBuffer){
        int remainingBytes = writeBuffer.remaining();

//        System.out.println(STR."BatchUtils type: \{BATCH_OF_EVENTS}");
        writeBuffer.put(BATCH_OF_EVENTS);
        // jump 2 integers
        writeBuffer.position(1 + JUMP);
//        System.out.println(STR."BatchUtils position after JUMP: \{writeBuffer.position()}");
        remainingBytes = remainingBytes - 1 - JUMP;

        // batch them all in the buffer,
        // until buffer capacity is reached or elements are all sent
        int count = 0;
        int idx = events.size() - remaining;
        while(idx < events.size() && remainingBytes > events.get(idx).totalSize()){
            TransactionEvent.writeWithinBatch( writeBuffer, events.get(idx) );
//            System.out.println(STR."BatchUtils position after event insert: \{writeBuffer.position()}");
            remainingBytes = remainingBytes - events.get(idx).totalSize();
            idx++;
            count++;
        }

        int position = writeBuffer.position();
        writeBuffer.putInt(1, position);
        writeBuffer.putInt(5, count);
        writeBuffer.position(position);

        System.out.println(STR."BatchUtils BATCH_OF_EVENTS event count: \{count}");

        return remaining - count;
    }

}
