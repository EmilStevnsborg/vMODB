package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

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

    private void disAssembleBatchPayload(ByteBuffer byteBuffer) throws IOException {
        final int JUMP = (2 * Integer.BYTES);
        while (byteBuffer.hasRemaining()) {
            byte type = byteBuffer.get();
            if (type != BATCH_OF_EVENTS) {
                throw new IOException("Unknown message type: " + type);
            }

            int segmentSize = byteBuffer.getInt();
            int eventCount = byteBuffer.getInt();

//            System.out.println(STR."ThesisLogger BATCH_OF_EVENTS event count: \{eventCount}");

            for (int i = 0; i < eventCount; i++) {
                long tid = byteBuffer.getLong();
                long batch = byteBuffer.getLong();
                int eventLength = byteBuffer.getInt();

//                System.out.println(STR."Logging event... tid: \{tid}, batch: \{batch}, eventLength: \{eventLength}");

                byte[] eventBytes = new byte[eventLength];
                byteBuffer.get(eventBytes);

                int payloadLength = byteBuffer.getInt();
                int payloadStart = byteBuffer.position();
                int payloadEnd = payloadStart + payloadLength;
//                System.out.println(STR."Logging event... payloadLength: \{payloadLength}, payloadStart: \{payloadStart}");

//                // this is just for testing
//                byte[] payloadBytes = new byte[payloadLength];
//                byteBuffer.get(payloadBytes);
//                String payload = new String(payloadBytes, StandardCharsets.UTF_8);
//                System.out.println(STR."Payload:\n\{payload}\n");
//                byteBuffer.position(payloadStart);

                int oldLimit = byteBuffer.limit();
                byteBuffer.limit(payloadEnd);
                while (byteBuffer.hasRemaining()) {
                    this.fileChannel.write(byteBuffer);
                }

                byteBuffer.limit(oldLimit);

//                System.out.println("Logging... Done writing payload to channel");
                byteBuffer.position(payloadEnd);

                int precedenceLength = byteBuffer.getInt();
                byte[] precedenceBytes = new byte[precedenceLength];
                byteBuffer.get(precedenceBytes);

//                System.out.println(STR."ThesisLogger DECODE position after decoding: \{byteBuffer.position()}");
            }
        }
    }

    @Override
    public void log(ByteBuffer byteBuffer) throws IOException {
        byte type = byteBuffer.get(0);
        switch (type) {
            // from and to server nodes
            case HEARTBEAT:
                System.out.println(STR."logging heartbeat, type=\{HEARTBEAT}");
                break;
            case PRESENTATION:
                System.out.println(STR."logging presentation, type=\{PRESENTATION}");
                break;
            case CONSUMER_SET:
                System.out.println(STR."logging consumer set, type=\{CONSUMER_SET}");
                break;

            // events
            case EVENT:
                System.out.println(STR."logging event, type=\{EVENT}");
                break;
            case BATCH_OF_EVENTS:
                System.out.println(STR."logging batch of events, type=\{BATCH_OF_EVENTS}");
                disAssembleBatchPayload(byteBuffer);
                break;
            case TX_ABORT:
                System.out.println(STR."logging transaction abort, type=\{TX_ABORT}");
                break;
            case BATCH_COMPLETE:
                System.out.println(STR."logging batch complete, type=\{BATCH_COMPLETE}");
                break;
            default:
                System.out.println(STR."logging unspecified event type: \{type}");
                break;
        }
        byteBuffer.clear();
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