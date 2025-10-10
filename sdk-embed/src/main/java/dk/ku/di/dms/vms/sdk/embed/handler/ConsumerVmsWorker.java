package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.logging.FromLogs;
import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Recovery;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.IChannel;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.WritePendingException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static dk.ku.di.dms.vms.sdk.embed.handler.ConsumerVmsWorker.State.*;
import static java.lang.System.Logger.Level.*;
import static java.lang.Thread.sleep;

/**
 * This thread encapsulates the batch of events sending task
 * that should occur periodically. Once set up, it schedules itself
 * after each run, thus avoiding duplicate runs of the same task.
 * -
 * I could get the connection from the vms...
 * But in the future, one output event will no longer map to a single vms
 * So it is better to make the event sender task complete enough
 * what happens if the connection fails? then put the list of events
 *  in the batch to resend or return to the original location. let the
 *  main loop schedule the timer again. set the network node to off
 */
public final class ConsumerVmsWorker extends StoppableRunnable implements IVmsContainer {

    private static final System.Logger LOGGER = System.getLogger(ConsumerVmsWorker.class.getName());

    private static final VarHandle WRITE_SYNCHRONIZER;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            WRITE_SYNCHRONIZER = l.findVarHandle(ConsumerVmsWorker.class, "writeSynchronizer", int.class);
        } catch (Exception e) {
            throw new InternalError(e);
        }
    }

    @SuppressWarnings("unused")
    private volatile int writeSynchronizer;

    private final VmsNode me;

    private final IdentifiableNode consumerVms;
    
    private final IChannel channel;
    private boolean consumerIsRecovering;
    private Consumer<Recovery.Payload> leaderQueueRecovery;
    private Supplier<long[]> getLatestCommittedInfo;

    private final ILoggingHandler loggingHandler;
    private final FromLogs fromLogs;

    private final IVmsSerdesProxy serdesProxy;

    private static final Deque<ByteBuffer> WRITE_BUFFER_POOL = new ConcurrentLinkedDeque<>();

    private final VmsEventHandler.VmsHandlerOptions options;

    private final Queue<ByteBuffer> loggingWriteBuffers = new ConcurrentLinkedQueue<>();

    private final Deque<ByteBuffer> pendingWritesBuffer = new ConcurrentLinkedDeque<>();

    private final MpscBlockingConsumerArrayQueue<TransactionEvent.PayloadRaw> transactionEventQueue;

    private State state;

    protected enum State { NEW, CONNECTED, DISCONNECTED, PRESENTATION_SENT, READY }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private final List<TransactionEvent.PayloadRaw> drained = new ArrayList<>(1024*10);

    public static ConsumerVmsWorker build(
                                  VmsNode me,
                                  IdentifiableNode consumerVms,
                                  Supplier<IChannel> channelSupplier,
                                  Consumer<Recovery.Payload> leaderQueueRecovery,
                                  Supplier<long[]> getLatestCommittedInfo,
                                  VmsEventHandler.VmsHandlerOptions options,
                                  IVmsSerdesProxy serdesProxy) {

        System.out.println(STR."ConsumerVmsWorker has been built for \{consumerVms.identifier}");
        return new ConsumerVmsWorker(me, consumerVms,
                channelSupplier.get(), leaderQueueRecovery,
                getLatestCommittedInfo, options, serdesProxy);
    }

    private ConsumerVmsWorker(VmsNode me,
                             IdentifiableNode consumerVms,
                             IChannel channel,
                             Consumer<Recovery.Payload> leaderQueueRecovery,
                             Supplier<long[]> getLatestCommittedInfo,
                             VmsEventHandler.VmsHandlerOptions options,
                             IVmsSerdesProxy serdesProxy) {
        this.me = me;
        this.consumerVms = consumerVms;
        this.channel = channel;
        this.consumerIsRecovering = false;
        this.leaderQueueRecovery = leaderQueueRecovery;
        this.getLatestCommittedInfo = getLatestCommittedInfo;

        ILoggingHandler loggingHandler;
        var logIdentifier = me.identifier+"_"+consumerVms.identifier;
        if(options.logging()){
            loggingHandler = LoggingHandlerBuilder.build(logIdentifier);
        } else {
            String loggingStr = System.getProperty("logging");
            if(Boolean.parseBoolean(loggingStr)){
                loggingHandler = LoggingHandlerBuilder.build(logIdentifier);
            } else {
                loggingHandler = new ILoggingHandler() { };
            }
        }

        this.loggingHandler = loggingHandler;
        this.serdesProxy = serdesProxy;
        this.fromLogs = new FromLogs(loggingHandler, serdesProxy);
        this.options = options;
        this.transactionEventQueue = new MpscBlockingConsumerArrayQueue<>(1024*1500);
        this.state = NEW;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void acquireLock(){
        while(! WRITE_SYNCHRONIZER.compareAndSet(this, 0, 1) );
    }

    public boolean tryAcquireLock(){
        return WRITE_SYNCHRONIZER.compareAndSet(this, 0, 1);
    }

    public void releaseLock(){
        // System.out.println(me.identifier +": Releasing lock");
        WRITE_SYNCHRONIZER.setVolatile(this, 0);
    }

    @Override
    public void run() {
        LOGGER.log(INFO, this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);
        if(!this.connect()) {
            LOGGER.log(WARNING, this.me.identifier+ ": Finishing prematurely worker for consumer VMS "+this.consumerVms.identifier+" because connection failed");
            return;
        }
        System.out.println(STR."consumerVmsWorker for consumer=\{consumerVms.identifier}, this.options.logging()=\{this.options.logging()}");
        if(this.options.logging()){
            this.eventLoopLogging();
        } else {
            this.eventLoopNoLogging();
        }
        LOGGER.log(INFO, this.me.identifier+ ": Finishing worker for consumer VMS: "+this.consumerVms.identifier);
    }

    private void eventLoopNoLogging() {
        while(this.isRunning()){
            try {
                this.drained.add(this.transactionEventQueue.take());
                this.transactionEventQueue.drain(this.drained::add);
                if(this.drained.size() == 1){
                    this.sendEventBlocking(this.drained.removeFirst());
                } else {
                    this.sendBatchOfEventsBlocking();
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+ ": Error captured in event loop (no logging) \n"+e);
            }
        }
    }

    private void eventLoopLogging() {
        int pollTimeout = 1;
        while (this.isRunning()){
            try {
                if (consumerIsRecovering)
                {
                    reconnect();
                }

                transactionEventQueue.drain(this.drained::add, this.options.networkBufferSize());
                if(this.drained.isEmpty()){
                    pollTimeout = Math.min(pollTimeout * 2, this.options.maxSleep());
                    this.processPendingLogging();
                    this.giveUpCpu(pollTimeout);
                    continue;
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
                this.sendBatchOfEventsNonBlocking();
                this.processPendingLogging();
            } catch (Exception e) {}
        }
    }


    private void reconnect() {
        System.out.println(STR."Reconnect to consumer VMS \{this.consumerVms.identifier} with state=\{this.state}");
        if (this.state == DISCONNECTED)
        {
            System.out.println(STR."Trying to connect to consumer VMS \{this.consumerVms.identifier}");
            var connected = connect();

            if (!connected) {
                try { sleep(2000); } catch (InterruptedException ignored) { }
                return;
            }
        }
        System.out.println(STR."Connected to \{this.consumerVms.identifier}");
        // send the events
        var latestCommittedInfo = this.getLatestCommittedInfo.get();
        var latestCommittedBatch = latestCommittedInfo[0];
//        sendUncommittedEvents(latestCommittedBatch);

        consumerIsRecovering = false;
        System.out.println(STR."Reconnected to \{this.consumerVms.identifier}");
    }

    /**
     * Responsible for making sure the handshake protocol is
     * successfully performed with a consumer VMS
     */
    private boolean connect() {
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize());
        try{
            NetworkUtils.configure(this.channel, options.soBufferSize());
            this.channel.connect(this.consumerVms.asInetSocketAddress()).get();
            this.state = CONNECTED;
            LOGGER.log(DEBUG,this.me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" status = "+this.state);
            String dataSchema = this.serdesProxy.serializeDataSchema(this.me.dataSchema);
            String inputEventSchema = this.serdesProxy.serializeEventSchema(this.me.inputEventSchema);
            String outputEventSchema = this.serdesProxy.serializeEventSchema(this.me.outputEventSchema);
            buffer.clear();
            Presentation.writeVms( buffer, this.me, this.me.identifier, this.me.batch, 0, this.me.previousBatch, dataSchema, inputEventSchema, outputEventSchema );
            buffer.flip();
            this.channel.write(buffer).get();
            this.state = PRESENTATION_SENT;
            LOGGER.log(DEBUG,me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" status = "+this.state);
            this.returnByteBuffer(buffer);
            LOGGER.log(INFO,me.identifier+ ": Setting up worker to send transactions to consumer VMS: "+this.consumerVms.identifier);
        } catch (Exception e) {
            // check if connection is still online. if so, try again
            // otherwise, retry connection in a few minutes
            System.out.println(STR."Could not connect to consumer \{consumerVms.identifier}");
            e.printStackTrace();
            LOGGER.log(ERROR, me.identifier + ": Caught an error while trying to connect to consumer VMS: " + this.consumerVms.identifier);
            return false;
        } finally {
            buffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(buffer);
        }
        this.state = READY;
        return true;
    }

    private void sendUncommittedEvents(long latestCommittedBatch)
    {
        ByteBuffer writeBuffer;
        long filePosition;
        filePosition = 0;
        while (filePosition != -1)
        {
            writeBuffer = retrieveByteBuffer();
            var segmentMetadata = fromLogs.loadSegment(writeBuffer, filePosition);
            filePosition = segmentMetadata.nextFilePosition;

            // ignore batches prior to latest committed batch
            if (segmentMetadata.bid <= latestCommittedBatch) {
                returnByteBuffer(writeBuffer);
                continue;
            }
            writeBuffer.flip();
            this.channel.write(writeBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
        }
    }

    // you are a consumer worker of the restarted VMS
    //      1. cut log of uncommitted batches
    @Override
    public void recover(long latestCommittedBatch)
    {
//        cutLog(latestCommittedBatch);
    }

    // consumer vms has crashed
    //      1. this may have not been registered yest
    public void initializeRecoveryInVms()
    {
        if (consumerIsRecovering) return;

        System.out.println("Initializing recovery in ConsumerVmsWorker");
        consumerIsRecovering = true;
        this.state = DISCONNECTED;

        // inform coordinator
        var recoveryMessage = Recovery.of(consumerVms);
        this.leaderQueueRecovery.accept(recoveryMessage);
    }

    // called from the VmsEventHandler
    @Override
    public void processRecoveryInVms()
    {
        // message from coordinator comes while already recovering
        if (consumerIsRecovering) return;

        consumerIsRecovering = true;
        this.state = DISCONNECTED;
    }

    private void processPendingLogging(){
        ByteBuffer writeBuffer;
        if((writeBuffer = this.loggingWriteBuffers.poll())!= null){
            try {
                writeBuffer.position(0);
                this.loggingHandler.log(writeBuffer);
                this.returnByteBuffer(writeBuffer);
            } catch (Exception e) {
                LOGGER.log(ERROR, me.identifier + ": Error on writing byte buffer to logging file: "+e.getMessage());
                e.printStackTrace(System.out);
                this.loggingWriteBuffers.add(writeBuffer);
            }
        }
    }

    private void processPendingWrites() {
        // do we have pending writes?
        ByteBuffer bb = this.pendingWritesBuffer.poll();
        if (bb != null) {
            LOGGER.log(INFO, me.identifier+": Retrying sending failed buffer to "+consumerVms.identifier);
            try {
                // sleep with the intention to let the OS flush the previous buffer
                try { sleep(100); } catch (InterruptedException ignored) { }
                this.acquireLock();
                this.channel.write(bb, options.networkSendTimeout(), TimeUnit.MILLISECONDS, bb, this.writeCompletionHandler);
            } catch (Exception e) {
                LOGGER.log(ERROR, me.identifier+": ERROR on retrying to send failed buffer to "+consumerVms.identifier+": \n"+e);
                if(e instanceof IllegalStateException){
                    LOGGER.log(INFO, me.identifier+": Connection to "+consumerVms.identifier+" is open? "+this.channel.isOpen());
                    // probably comes from the class {@AsynchronousSocketChannelImpl}:
                    // "Writing not allowed due to timeout or cancellation"
                    this.stop();
                }
                this.releaseLock();
                bb.clear();
                this.pendingWritesBuffer.add(bb);
            }
        }
    }

    // TODO sending batch of events (look for format)
    private void sendBatchOfEventsNonBlocking() {
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        while(remaining > 0){
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, this.drained, writeBuffer); // TODO putting payloads from drained into buffer
                writeBuffer.flip();
                LOGGER.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;
                // maximize useful work
                while(!this.tryAcquireLock()){
                    this.processPendingLogging();
                }
                // write drained in parts over channel. Continuously add the buffer for each part to logging
                // once all parts have been written clear the drained
                this.channel.write(writeBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                remaining = 0;
                this.releaseLock();
            }
        }
        this.drained.clear();
    }

    private void sendEventBlocking(TransactionEvent.PayloadRaw payload) {
        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        try {
            TransactionEvent.write( writeBuffer, payload );
            writeBuffer.flip();
            do {
                this.channel.write(writeBuffer).get();
            } while (writeBuffer.hasRemaining());
        } catch (Exception e){
            if(this.isRunning()) {
                LOGGER.log(ERROR, "Error caught on sending single event: " + e);
                this.transactionEventQueue.offer(payload);
            }
            writeBuffer.clear();
        }
        finally {
            this.returnByteBuffer(writeBuffer);
        }
    }

    private void sendBatchOfEventsBlocking() {
        System.out.println("ConsumerVmsWorker.sendBatchOfEventsBlocking");
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        while(remaining > 0){
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, this.drained, writeBuffer);
                writeBuffer.flip();
                LOGGER.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;
                do {
                    this.channel.write(writeBuffer).get();
                } while(writeBuffer.hasRemaining());
                this.returnByteBuffer(writeBuffer);
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                // force loop exit
                remaining = 0;
            }
        }
        this.drained.clear();
    }

    private void failSafe(Exception e, ByteBuffer writeBuffer) {
        LOGGER.log(ERROR, this.me.identifier+ ": Error submitting events to "+this.consumerVms.identifier+"\n"+ e);
        // return non-processed events to original location or what?
        if (!this.channel.isOpen()) {
            LOGGER.log(WARNING, "The "+this.consumerVms.identifier+" VMS is offline");
        }
        // return events to the deque
        this.transactionEventQueue.addAll(this.drained);
        if(writeBuffer != null) {
            this.returnByteBuffer(writeBuffer);
        }
    }

    private ByteBuffer retrieveByteBuffer(){
        ByteBuffer bb = WRITE_BUFFER_POOL.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(this.options.networkBufferSize());
    }

    // Adding
    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        WRITE_BUFFER_POOL.add(bb);
    }

    private final class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            LOGGER.log(DEBUG, me.identifier + ": Batch with size " + result + " has been sent to: " + consumerVms.identifier);
            if(byteBuffer.hasRemaining()){
                LOGGER.log(WARNING, me.identifier + ": Remaining bytes will be sent to: " + consumerVms.identifier);
                // keep the lock and send the remaining
                channel.write(byteBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, byteBuffer, this);
            } else {
                if(options.logging()){
//                    System.out.println("ConsumerVmsWorker.WriteCompletionHandler.completed: adding bytebuffer to loggingBuffers");
                    loggingWriteBuffers.add(byteBuffer);
                } else {
                    returnByteBuffer(byteBuffer);
                }
                releaseLock();
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            System.out.println(STR."(ConsumerVmsWorker) Writing to Consumer \{consumerVms.identifier} failed");

            if (exc instanceof AsynchronousCloseException) {
                System.out.println("Channel failed due to AsynchronousCloseException");
                initializeRecoveryInVms();
            }
            else if (exc instanceof ClosedChannelException) {
                System.out.println("Channel failed due to ClosedChannelException");
                initializeRecoveryInVms();
            }
            else if (exc instanceof WritePendingException) {
                System.out.println("Channel failed due to WritePendingException");
                initializeRecoveryInVms();
            }
            else if (exc instanceof IOException) {
                System.out.println("Channel failed due to IOException");
                initializeRecoveryInVms();
            }
            else {
                System.out.println(STR."Channel failed due to something else \{exc.getMessage()}");
            }

            releaseLock();
            LOGGER.log(ERROR, me.identifier+": ERROR on writing batch of events to "+consumerVms.identifier+": \n"+exc);
            byteBuffer.position(0);
            pendingWritesBuffer.add(byteBuffer);
            LOGGER.log(INFO, me.identifier + ": Byte buffer added to pending queue. #pending: "+ pendingWritesBuffer.size());
        }
    }

    @Override
    public void queue(TransactionEvent.PayloadRaw eventPayload){
        if(!this.transactionEventQueue.offer(eventPayload)){
            System.out.println(me.identifier +": cannot add event in the input queue");
        }
    }

    @Override
    public int cutLog(long failedTid, long batch) {
        var placeHolderBuffer = this.retrieveByteBuffer();
        System.out.println("ConsumerVmsWorker is cutting log");
        int tidsLeftInBatch = -1;
        try {
            // clear the transactionInput queue
            transactionEventQueue.clear();

            // some events may be in memory,
            drained.clear();

            // some may be in buffers not logged yet,
            loggingWriteBuffers.stream().forEach(buffer -> returnByteBuffer(buffer));

            // some may already have been logged
            System.out.println("ConsumerVmsWorker is cutting the persistent log");
            tidsLeftInBatch = loggingHandler.cutLog(placeHolderBuffer, failedTid, batch);

        } catch (Exception e) {
            System.out.println("CUTTING THE LOG CAUSED AN EXCEPTION");
        }
        returnByteBuffer(placeHolderBuffer);
        return tidsLeftInBatch;
    }

    @Override
    public String identifier() {
        return this.consumerVms.identifier;
    }

    @Override
    public void stop(){
        super.stop();
        this.channel.close();
    }

}