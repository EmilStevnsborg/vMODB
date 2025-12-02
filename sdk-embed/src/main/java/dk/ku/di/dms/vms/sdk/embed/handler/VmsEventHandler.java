package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import dk.ku.di.dms.vms.modb.common.logging.LongPairStore;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.*;
import dk.ku.di.dms.vms.modb.common.schema.network.control.*;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbortAck;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbortInfo;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.IVmsTransactionResult;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.web_common.HttpUtils;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import dk.ku.di.dms.vms.web_common.ModbHttpServer;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.JdkAsyncChannel;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static java.lang.System.Logger.Level.*;

/**
 * This default event handler connects direct to the coordinator
 * So in this approach it bypasses the sidecar. In this way,
 * the DBMS must also be run within this code.
 * The virtual microservice doesn't know who is the coordinator. It should be passive.
 * The leader and followers must share a list of VMSs.
 * Could also try to adapt to JNI:
 * <a href="https://nachtimwald.com/2017/06/17/calling-java-from-c/">...</a>
 */
public final class VmsEventHandler extends ModbHttpServer {

    private static final System.Logger LOGGER = System.getLogger(VmsEventHandler.class.getName());

    /** SERVER SOCKET **/
    // other VMSs may want to connect in order to send events
    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    /** INTERNAL CHANNELS **/
    private final VmsEmbedInternalChannels vmsInternalChannels;

    /** VMS METADATA **/
    private final VmsNode me; // this merges network and semantic data about the vms

    private final VmsRuntimeMetadata vmsMetadata;

    /** EXTERNAL VMSs **/
    private final Map<String, List<IVmsContainer>> eventToConsumersMap;
    private final Map<String, List<String>> consumerToEventsMap;

    // vms identifier to node
    private final Map<String, IdentifiableNode> vmsMetadataMap;

    // built while connecting to the consumers
    private final Map<IdentifiableNode, IVmsContainer> consumerVmsContainerMap;

    // built dynamically as new producers request connection
    private final Map<Integer, ConnectionMetadata> producerConnectionMetadataMap;

    /** For checkpointing the state */
    private final ITransactionManager transactionManager;

    /** SERIALIZATION & DESERIALIZATION **/
    private final IVmsSerdesProxy serdesProxy;
    private final boolean recoveryEnabled;

    private final VmsHandlerOptions options;

    private final IHttpHandler httpHandler;

    /** COORDINATOR **/
    private ServerNode leader;

    private ConnectionMetadata leaderConnectionMetadata;

    // the thread responsible to send data to the leader
    private LeaderWorker leaderWorker;

    // refer to what operation must be performed
    // private final BlockingQueue<Object> leaderWorkerQueue;

    // cannot be final, may differ across time and new leaders
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final Set<String> queuesLeaderSubscribesTo;

    /** INTERNAL STATE **/

    // metadata about all non-committed batches.
    // when a batch commit finishes, it should be removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    /**
     * It marks how many TIDs that the scheduler has executed.
     * The scheduler is batch-agnostic. That means in order
     * to progress with the batch in the event handler, we need to check if the
     * batch has completed using the number of TIDs executed.
     * It must be separated from batchContextMap due to different timing of batch context sending
     */
    public final Map<Long, BatchMetadata> trackingBatchMap;

    public static final class BatchMetadata {
        public int numberTIDsExecuted;
        public long maxTidExecuted;
    }

    /**
     * It is necessary a way to store the tid received to a
     * corresponding dependence map.
     */
    private final Map<Long, Map<String, Long>> tidToPrecedenceMap;

    private Consumer<Boolean> pauseHandler;
    private BiFunction<Long, Long, Long[]> taskClearer;
    private BiConsumer<Long, Long> recoveryHandler;

    private ILoggingHandler loggingHandler;

    private LongPairStore commitInfo;
    public Map<Long, Set<Long>> batchAbortedTIDs;
    public long generation;
    public static VmsEventHandler build(// to identify which vms this is
                                        VmsNode me,
                                        // to checkpoint private state
                                        ITransactionManager transactionalHandler,
                                        // for communicating with other components
                                        VmsEmbedInternalChannels vmsInternalChannels,
                                        // metadata about this vms
                                        VmsRuntimeMetadata vmsMetadata,
                                        VmsApplicationOptions options,
                                        IHttpHandler httpHandler,
                                        // serialization/deserialization of objects
                                        IVmsSerdesProxy serdesProxy) {

        return build(me, transactionalHandler, vmsInternalChannels, vmsMetadata, options, httpHandler, serdesProxy, false);
    }

    public static VmsEventHandler build(// to identify which vms this is
                                        VmsNode me,
                                        // to checkpoint private state
                                        ITransactionManager transactionalHandler,
                                        // for communicating with other components
                                        VmsEmbedInternalChannels vmsInternalChannels,
                                        // metadata about this vms
                                        VmsRuntimeMetadata vmsMetadata,
                                        VmsApplicationOptions options,
                                        IHttpHandler httpHandler,
                                        // serialization/deserialization of objects
                                        IVmsSerdesProxy serdesProxy,
                                        boolean recoveryEnabled){
        try {
            return new VmsEventHandler(me, vmsMetadata,
                    transactionalHandler, vmsInternalChannels,
                    new VmsEventHandler.VmsHandlerOptions( options.maxSleep(), options.networkBufferSize(),
                            options.soBufferSize(), options.networkThreadPoolSize(), options.networkSendTimeout(),
                            options.vmsThreadPoolSize(), options.numVmsWorkers(), options.isLogging(), options.isCheckpointing()),
                    httpHandler, serdesProxy, recoveryEnabled);
        } catch (IOException e){
            throw new RuntimeException("Error on setting up event handler: "+e.getCause()+ " "+ e.getMessage());
        }
    }

    public record VmsHandlerOptions(int maxSleep,
                                    int networkBufferSize,
                                    int soBufferSize,
                                    int networkThreadPoolSize,
                                    int networkSendTimeout,
                                    int vmsThreadPoolSize,
                                    int numVmsWorkers,
                                    boolean logging,
                                    boolean checkpointing) {}

    private VmsEventHandler(VmsNode me,
                            VmsRuntimeMetadata vmsMetadata,
                            ITransactionManager transactionManager,
                            VmsEmbedInternalChannels vmsInternalChannels,
                            VmsHandlerOptions options,
                            IHttpHandler httpHandler,
                            IVmsSerdesProxy serdesProxy,
                            boolean recoveryEnabled) throws IOException {
        super();

        // network and executor
        if(options.networkThreadPoolSize > 0){
            // at least two, one for acceptor and one for new events
            this.group = AsynchronousChannelGroup.withFixedThreadPool(
                    options.networkThreadPoolSize,
                    Thread.ofPlatform().name("vms-network-thread").factory()
            );
            this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        } else {
            this.group = null;
            this.serverSocket = AsynchronousServerSocketChannel.open(null);
        }
        this.serverSocket.bind(me.asInetSocketAddress());

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;

        this.vmsMetadata = vmsMetadata;
        // no concurrent threads modifying them
        this.eventToConsumersMap = new HashMap<>();
        this.consumerToEventsMap = new HashMap<>();
        this.vmsMetadataMap = new HashMap<>();
        this.consumerVmsContainerMap = new HashMap<>();
        // concurrent threads modifying it
        this.producerConnectionMetadataMap = new ConcurrentHashMap<>();

        this.serdesProxy = serdesProxy;
        this.recoveryEnabled = recoveryEnabled;

        this.batchContextMap = new ConcurrentHashMap<>();
        this.trackingBatchMap = new ConcurrentHashMap<>();
        this.tidToPrecedenceMap = new ConcurrentHashMap<>();

        this.transactionManager = transactionManager;

        // set leader off at the start
        this.leader = new ServerNode("0.0.0.0",0);
        this.leader.off();

        this.queuesLeaderSubscribesTo = new HashSet<>();

        this.options = options;
        this.httpHandler = httpHandler;

        var logIdentifier = STR."\{me.identifier}_out";
        var truncate = recoveryEnabled ? false : true;
        this.loggingHandler = LoggingHandlerBuilder.build(logIdentifier, serdesProxy, options.networkBufferSize, truncate);

        this.commitInfo = new LongPairStore(STR."\{me.identifier}_commit_info", !recoveryEnabled);
        this.batchAbortedTIDs = new HashMap<>();
        this.generation = 0;
    }

    @Override
    public void run() {
        // setup accept since we need to accept connections from the coordinator and other VMSs
        this.serverSocket.accept(null, new AcceptCompletionHandler());
        var crashOccurred = true; // check for logs or snapshot

        System.out.println(STR."crashOccurred=\{crashOccurred} && recoveryEnabled=\{recoveryEnabled}");
        if (crashOccurred && recoveryEnabled) {
            // need to be in configuration
            recoverVms("localhost", 8766);
//            recoverVms(leader.host, leader.port);
        }
    }

    public void AddSchedulerPauseHandler(Consumer<Boolean> pauseHandler){
        this.pauseHandler = pauseHandler;
    }

    public void AddSchedulerTaskClearer(BiFunction<Long, Long, Long[]> taskClearer){
        this.taskClearer = taskClearer;
    }
    public void AddSchedulerRecoveryHandler(BiConsumer<Long, Long> recoveryHandler){
        this.recoveryHandler = recoveryHandler;
    }
    private void fixTrackingBatch(long batch, long numberTIDsExecuted, long maxTidExecuted)
    {
        // System.out.println(STR."fixing tracking batch \{batch} in \{me.identifier}");
        var trackedBatchKeys = trackingBatchMap.keySet();
        for(var trackedBatchKey : trackedBatchKeys) {
            if (trackedBatchKey < batch) continue;
            if (trackedBatchKey > batch) {
                trackingBatchMap.put(trackedBatchKey, new BatchMetadata());
                continue;
            }

            // trackedBatchKey == failedTidBatch
            var failedTidBatchMetadata = trackingBatchMap.get(trackedBatchKey);

            // set numTIDsExecuted based executed tasks in scheduler
            failedTidBatchMetadata.numberTIDsExecuted = (int) numberTIDsExecuted;
            failedTidBatchMetadata.maxTidExecuted = maxTidExecuted;

            // System.out.println(STR."\{me.identifier}-FIX TRACKING BATCH numberTIDsExecuted=\{numberTIDsExecuted} and maxTidExecuted=\{maxTidExecuted}");
        }
    }

    // crashed VMS tries to recover
    private void recoverVms(String leaderHost, int leaderPort)
    {
        System.out.println(STR."\{me.identifier} started, and is now recovering. It will ping leader at \{leaderHost}:\{leaderPort}");
        pauseHandler.accept(true);

        // fix scheduler metadata
        try {
            var latestCommitInfo = commitInfo.getLatest();
            var batch = latestCommitInfo[0];
            var maxTid = latestCommitInfo[1];
            recoveryHandler.accept(batch, maxTid);
        } catch (Exception e) {
            //
        }

        // connect to leader
        try {
            var leaderAsInetSocketAddress = new NetworkAddress("localhost", leaderPort);
            connectToCoordinator(leaderAsInetSocketAddress);
        } catch (Exception e) {
            System.out.println(STR."\{me.identifier} Failed to reconnect to coordinator");
        }

        pauseHandler.accept(false);
    }

    // this function only updates in memory structures
    private void resetToCommittedState()
    {
        // System.out.println(STR."\{me.identifier} aborts all uncommitted transactions");
        pauseHandler.accept(true);

        // update epoch


        var latestCommitInfo = commitInfo.getLatest();
        var batch = latestCommitInfo[0];
        var maxTid = latestCommitInfo[1];

        System.out.println(STR."\{me.identifier} resets to state batch=\{batch} and tid=\{maxTid}");

        // clear transaction event queues
//        consumerVmsContainerMap.values().forEach(worker -> {
//            worker.clear();
//        });

        // clear all tasks at the maxTID or later
        taskClearer.apply(maxTid+1, batch+1);

        loggingHandler.cutLog(maxTid+1);
        restoreStableState(maxTid+1);

        // clearing internal queue entirely
        // needs to be done reliably
        // vmsInternalChannels.transactionInputQueue().clear();

        trackingBatchMap.clear();
        batchContextMap.entrySet().removeIf(entry -> entry.getKey() > batch);

        pauseHandler.accept(false);
    }

    private void processVmsCrash(VmsCrash.Payload vmsCrash)
    {
        System.out.println(STR."\{me.identifier} started processing crash of \{vmsCrash}, new gen is \{vmsCrash.newGeneration()}");
        var vmsNode = vmsMetadataMap.get(vmsCrash.vms());
        var crashedVmsWorkerContainer = consumerVmsContainerMap.remove(vmsNode);
        if (crashedVmsWorkerContainer != null) {
            crashedVmsWorkerContainer.stop();

            for (var entry : eventToConsumersMap.entrySet()) {
                var eventConsumerWorkers = entry.getValue();
                var sizeBefore = eventConsumerWorkers.size();
                eventConsumerWorkers.remove(crashedVmsWorkerContainer);

//                System.out.println(STR."\{me.identifier} removes \{crashedVmsWorkerContainer.identifier()} " +
//                        STR."from eventConsumerWorkers, size before=\{sizeBefore}" +
//                        STR."from eventConsumerWorkers, size after=\{eventConsumerWorkers.size()}");
            }
        }

        // updating the generation of events being expected
        this.generation = vmsCrash.newGeneration();

                // abort uncommitted events
        resetToCommittedState();

        var crashAck = CrashAck.of(vmsCrash.vms(), me.identifier);
        leaderWorker.queueMessage(crashAck);
    }

    // vms is back online, this vms is a producer and must reconnect
    private void processVmsReconnection(VmsReconnect.Payload reconnection)
    {
        var restartedVms = reconnection.vms();
        System.out.println(STR."\{me.identifier} initiates reconnection to consumer \{restartedVms}");

        // init new worker
        var restartedVmsNode = vmsMetadataMap.get(restartedVms);
        initConsumerVmsWorker(restartedVmsNode, consumerToEventsMap.get(restartedVms), 1); // mauybe identifier should change?

        var reconnectionACK = ReconnectionAck.of(restartedVms, me.identifier);
        leaderWorker.queueMessage(reconnectionACK);
    }


    private void applyAbortLocally(long tid, long bid, boolean initiatedAbort)
    {
        // System.out.println(STR."\{me.identifier} APPLIES abort for \{tid}");
        pauseHandler.accept(true);

        batchAbortedTIDs.putIfAbsent(bid, new HashSet());
        batchAbortedTIDs.get(bid).add(tid);

        if (batchContextMap.containsKey(bid)) {
            var batchContext = batchContextMap.get(bid);

            if (!batchContext.abortedTIDs.contains(tid)) {
                batchContext.numberOfTIDsBatch -= 1;
                batchContext.abortedTIDs.add(tid);
                batchContextMap.put(bid, batchContext);
            }
        }

        // clear tasks
        var metadata = taskClearer.apply(tid, bid);

        // including removing tid
        loggingHandler.cutLog(tid);

        // including removing tid
        restoreStableState(tid);

        // input queue clear??? (an event can't be in the queue unless it was processed upstream??)
        // if A->B->C, and 10 aborts in C, then 30 may be in the queue, 30 will still be sent by B?
        // this is not updating the queue
        // correctness relies on the scheduler to consider old events as deprecated
        vmsInternalChannels.transactionInputQueue().stream().filter(input -> input.tid() >= tid);

        var numTIDsExecuted = metadata[0];
        var maxTidExecuted = metadata[1];
        fixTrackingBatch(bid, numTIDsExecuted, maxTidExecuted);

        pauseHandler.accept(false);
    }

    // for affected vms
    private void processAbort(TransactionAbort.Payload txAbort)
    {
        // has this VMS seen the abort? optimization

        var tid = txAbort.tid();
        var bid = txAbort.batch();
        var abortedTIDs = batchAbortedTIDs.get(bid);
        if (abortedTIDs != null && abortedTIDs.contains(tid))
        {
            var abortMessage = TransactionAbortInfo.of(bid, tid, me.identifier);
            this.leaderWorker.queueMessage(abortMessage);
            return;
        }

        System.out.println(STR."\{me.identifier} PROCESSES abort of \{tid}");

        applyAbortLocally(tid, bid, false);

        var abortMessage = TransactionAbortAck.of(bid, tid, me.identifier);
        this.leaderWorker.queueMessage(abortMessage);
    }

    // for vms that failed
    private void abortTransaction(IVmsTransactionResult txResult)
    {
        // get event
        var eventOutput = txResult.getOutboundEventResult();

        var tid = eventOutput.tid();
        var bid = eventOutput.batch();

        System.out.println(STR."\{me.identifier} INITIATES abort of \{tid}");

        applyAbortLocally(tid, bid, true);

        var abortMessage = TransactionAbortInfo.of(bid, tid, me.identifier);
        this.leaderWorker.queueMessage(abortMessage);
    }

    public void processOutputEvent(IVmsTransactionResult txResult) {
        // System.out.println(STR."new transaction result in \{me.identifier} for tid=\{txResult.tid()}");

        if (txResult.getOutboundEventResult().isAbort()) {
            abortTransaction(txResult);
            return;
        }

        // it is a void method that executed, nothing to send
        if (txResult.getOutboundEventResult().outputQueue() != null) {
            Map<String, Long> precedenceMap = this.tidToPrecedenceMap.get(txResult.tid());
            if (precedenceMap != null) {
                // remove ourselves (which also saves some bytes)
                precedenceMap.remove(this.me.identifier);
                String precedenceMapUpdated = this.serdesProxy.serializeMap(precedenceMap);
                this.processOutputEvent(txResult.getOutboundEventResult(), precedenceMapUpdated);
            } else {
                LOGGER.log(ERROR, this.me.identifier + ": No precedence map found for TID: " + txResult.tid());
            }
        }
        // scheduler can be way ahead of the last batch committed
        this.updateBatchStats(txResult.getOutboundEventResult());
    }

    /**
     * Many outputs from the same transaction may arrive here concurrently,
     * but can only send the batch commit once
     */
    private void updateBatchStats(OutboundEventResult outputEvent) {
        BatchMetadata batchMetadata = this.updateBatchMetadataAtomically(outputEvent);
        // not arrived yet
        if(!this.batchContextMap.containsKey(outputEvent.batch())) return;
        BatchContext thisBatch = this.batchContextMap.get(outputEvent.batch());
        if(thisBatch.numberOfTIDsBatch != batchMetadata.numberTIDsExecuted) {
            return;
        }
        LOGGER.log(DEBUG, this.me.identifier + ": All TIDs for the batch " + thisBatch.batch + " have been executed");
        thisBatch.setStatus(BatchContext.BATCH_COMPLETED);
        // if terminal, must send batch complete
        if (thisBatch.terminal) {
            LOGGER.log(DEBUG, this.me.identifier + ": Requesting leader worker to send batch " + thisBatch.batch + " complete");
            // must be queued in case leader is off and comes back online
            this.leaderWorker.queueMessage(BatchComplete.of(thisBatch.batch, this.me.identifier));
        }
    }

    private BatchMetadata updateBatchMetadataAtomically(OutboundEventResult outputEvent) {
        return this.trackingBatchMap.compute(outputEvent.batch(),
                (ignored, y) -> {
                    BatchMetadata toMod = y;
                    if(toMod == null){
                        toMod = new BatchMetadata();
                    }
                    toMod.numberTIDsExecuted += 1;
                    if(toMod.maxTidExecuted < outputEvent.tid()){
                        toMod.maxTidExecuted = outputEvent.tid();
                    }
                    return toMod;
                });
    }

    private void connectToCoordinator(NetworkAddress leaderAddress) throws IOException, InterruptedException, ExecutionException
    {
        var channel = JdkAsyncChannel.create(this.group);
        NetworkUtils.configure(channel, options.soBufferSize());
        channel.connect(leaderAddress.asInetSocketAddress()).get();

        var buffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize());
        buffer.put(VMS_RECONNECTION);
        me.asIdentifiableNode().write(buffer); // this can be hacked
        buffer.flip();
        channel.write(buffer);
    }

    private void connectToReceivedConsumerSet(Map<String, List<IdentifiableNode>> receivedConsumerVms) {
        Map<IdentifiableNode, List<String>> consumerToEventsMap = new HashMap<>();
        // build an indirect map
        for(Map.Entry<String,List<IdentifiableNode>> entry : receivedConsumerVms.entrySet()) {
            for(IdentifiableNode consumer : entry.getValue()){
                consumerToEventsMap.computeIfAbsent(consumer, (ignored) -> new ArrayList<>()).add(entry.getKey());
            }
        }
        for( Map.Entry<IdentifiableNode,List<String>> consumerEntry : consumerToEventsMap.entrySet() ) {
            // connect to more consumers...
            for(int i = 0; i < this.options.numVmsWorkers; i++){
                this.initConsumerVmsWorker(consumerEntry.getKey(), consumerEntry.getValue(), i);
            }
        }
    }

    private static final boolean INFORM_BATCH_ACK = false;

    private void checkpoint(long batch, long maxTid) {
        // System.out.println(STR."checkpointing \{maxTid} in \{me.identifier}");
        //this.batchContextMap.get(batch).setStatus(BatchContext.CHECKPOINTING);
        // of course, I do not need to stop the scheduler on commit
        // I need to make access to the data versions data race free
        // so new transactions get data versions from the version map or the store
        //long initTs = System.currentTimeMillis();
        this.transactionManager.checkpoint(maxTid);
        //LOGGER.log(WARNING, me.identifier+": Checkpointing latency is "+(System.currentTimeMillis()-initTs));
        this.batchContextMap.get(batch).setStatus(BatchContext.BATCH_COMMITTED);
        // it may not be necessary. the leader has already moved on at this point
        if(INFORM_BATCH_ACK) {
            this.leaderWorker.queueMessage(BatchCommitAck.of(batch, this.me.identifier));
        }
    }


    private void restoreStableState(long failedTid)
    {
        transactionManager.restoreStableState(failedTid);
    }
    /**
     * It creates the payload to be sent downstream
     * @param outputEvent the event to be sent to the respective consumer vms
     */
    private void processOutputEvent(OutboundEventResult outputEvent, String precedenceMap){
        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(outputEvent.outputQueue());
        /*
         * does the leader consumes this queue?
        if( this.queuesLeaderSubscribesTo.contains( outputEvent.outputQueue() ) ){
            logger.log(DEBUG,me.identifier+": An output event (queue: "+outputEvent.outputQueue()+") will be queued to leader");
            this.leaderWorkerQueue.add(new LeaderWorker.Message(SEND_EVENT, payload));
        }
        */
        List<IVmsContainer> consumerVMSs = this.eventToConsumersMap.get(outputEvent.outputQueue());
        if(consumerVMSs == null || consumerVMSs.isEmpty()){
            LOGGER.log(DEBUG,STR."\{me.identifier} processed \{outputEvent.tid()} but consumerVMSs is empty or null");
            return;
        }

        // has not aborted
        String payloadpayload = this.serdesProxy.serialize(outputEvent.output(), clazz);
        TransactionEvent.PayloadRaw payload =
                TransactionEvent.of(outputEvent.tid(), outputEvent.batch(),
                        outputEvent.generation(),
                        outputEvent.outputQueue(), payloadpayload,
                        precedenceMap);

        loggingHandler.log(payload);

        for(IVmsContainer consumerVmsContainer : consumerVMSs) {
            // System.out.println(STR."\{me.identifier} queuing \{outputEvent.tid()} for \{consumerVmsContainer.identifier()}");
            consumerVmsContainer.queue(payload);
        }
    }

    public void initConsumerVmsWorker(IdentifiableNode node, List<String> outputEvents, int identifier){

        this.consumerToEventsMap.computeIfAbsent(node.identifier, (ignored) -> new CopyOnWriteArrayList<>());

        if(this.producerConnectionMetadataMap.containsKey(node.hashCode())){
            LOGGER.log(WARNING,"The node "+ node.host+" "+ node.port+" already contains a connection as a producer");
        }
        if(this.me.hashCode() == node.hashCode()){
            LOGGER.log(ERROR,this.me.identifier+" is receiving itself as consumer: "+ node.identifier);
            return;
        }
        ConsumerVmsWorker consumerVmsWorker = ConsumerVmsWorker.build(this.me, node,
                () -> JdkAsyncChannel.create(this.group),
                this.options,
                this.serdesProxy);
        Thread.ofPlatform().name("vms-consumer-"+node.identifier+"-"+identifier)
                .inheritInheritableThreadLocals(false)
                .start(consumerVmsWorker);
        if(!this.consumerVmsContainerMap.containsKey(node)){
            if(this.options.numVmsWorkers == 1) {
                vmsMetadataMap.put(node.identifier, node);
                this.consumerVmsContainerMap.put(node, consumerVmsWorker);
            } else {
                vmsMetadataMap.put(node.identifier, node);
                MultiVmsContainer multiVmsContainer = new MultiVmsContainer(consumerVmsWorker, node, this.options.numVmsWorkers);
                this.consumerVmsContainerMap.put(node, multiVmsContainer);
            }
            // add to tracked VMSs
            for (String outputEvent : outputEvents) {
                LOGGER.log(INFO,me.identifier+ " adding "+outputEvent+" to consumers map with "+node.identifier);
                this.eventToConsumersMap.computeIfAbsent(outputEvent, (ignored) -> new ArrayList<>());
                this.eventToConsumersMap.get(outputEvent).add(consumerVmsWorker);

                this.consumerToEventsMap.get(node.identifier).add(outputEvent);
            }
        } else {
            IVmsContainer vmsContainer = this.consumerVmsContainerMap.get(node);
            if(vmsContainer instanceof MultiVmsContainer multiVmsContainer){
                multiVmsContainer.addConsumerVms(consumerVmsWorker);
            } else {
                // stop previous, replace by the new one
                vmsContainer.stop();
                vmsMetadataMap.put(node.identifier, node);
                this.consumerVmsContainerMap.put(node, consumerVmsWorker);
            }
        }
        // set up read from consumer vms? we read nothing from consumer vms. maybe in the future can negotiate amount of data to avoid performance problems
        // channel.read(buffer, 0, new VmsReadCompletionHandler(this.node, connMetadata, buffer));
    }

    /**
     * The completion handler must execute fast
     */
    private final class VmsReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        // the VMS sending events to me
        private final IdentifiableNode node;
        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;

        public VmsReadCompletionHandler(IdentifiableNode node,
                                        ConnectionMetadata connectionMetadata,
                                        ByteBuffer byteBuffer){
            this.node = node;
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = byteBuffer;
            LIST_BUFFER.add(new ArrayList<>(1024));
        }

        @Override
        public void completed(Integer result, Integer startPos) {
            if(result == -1){
                // end-of-stream signal, no more data can be read
                LOGGER.log(WARNING,me.identifier+": VMS "+node.identifier+" has disconnected!");
                try {
                    this.connectionMetadata.channel.close();
                } catch (IOException ignored) { }
                return;
            }
            if(startPos == 0){
                this.readBuffer.flip();
            }
            byte messageType = this.readBuffer.get();
            switch (messageType) {
                case (BATCH_OF_EVENTS) -> {
                    int bufferSize = this.getBufferSize();
                    if(this.readBuffer.remaining() < bufferSize){
                        this.fetchMoreBytes(startPos);
                        return;
                    }
                    this.processBatchOfEvents(this.readBuffer);
                }
                case (EVENT) -> {
                    int bufferSize = this.getBufferSize();
                    if(this.readBuffer.remaining() < bufferSize){
                        this.fetchMoreBytes(startPos);
                        return;
                    }
                    this.processSingleEvent(this.readBuffer);
                }
                default -> {
                    LOGGER.log(ERROR,me.identifier+": Unknown message type "+messageType+" received from: "+node.identifier);
                    if(!isRunning()){
                        // avoid spamming the logging
                        return;
                    }
                }
            }
            if(this.readBuffer.hasRemaining()){
                this.completed(result, this.readBuffer.position());
            } else {
                this.setUpNewRead();
            }
        }

        private int getBufferSize() {
            int bufferSize = Integer.MAX_VALUE;
            // check if we can read an integer
            if(this.readBuffer.remaining() > Integer.BYTES) {
                // size of the batch
                bufferSize = this.readBuffer.getInt();
                // discard message type and size of batch from the total size since it has already been read
                bufferSize -= 1 + Integer.BYTES;
            }
            return bufferSize;
        }

        private void fetchMoreBytes(Integer startPos) {
            this.readBuffer.position(startPos);
            this.readBuffer.compact();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void setUpNewRead() {
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        // dont add events while crash is being processed maybe?
        private void processSingleEvent(ByteBuffer readBuffer) {
            try {
                TransactionEvent.Payload payload = TransactionEvent.read(readBuffer);
                LOGGER.log(DEBUG,me.identifier+": 1 event received from "+node.identifier+"\n"+payload);
                // send to scheduler
                if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                    InboundEvent inboundEvent = buildInboundEvent(payload);
                    vmsInternalChannels.transactionInputQueue().add(inboundEvent);
                }
            } catch (Exception e) {
                if(e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading event: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            }
        }

        private void processBatchOfEvents(ByteBuffer readBuffer) {
            List<InboundEvent> inboundEvents = LIST_BUFFER.poll();
            if(inboundEvents == null) inboundEvents = new ArrayList<>(1024);
            try {
                int count = readBuffer.getInt();
                LOGGER.log(DEBUG,me.identifier + ": Batch of [" + count + "] events received from " + node.identifier);
                TransactionEvent.Payload payload;
                int i = 0;
                while (i < count) {
                    payload = TransactionEvent.read(readBuffer);
                    LOGGER.log(DEBUG, me.identifier+": Processed TID "+payload.tid());
                    if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                        InboundEvent inboundEvent = buildInboundEvent(payload);
                        inboundEvents.add(inboundEvent);
                    }
                    i++;
                }
                if(count != inboundEvents.size()){
                    LOGGER.log(WARNING,me.identifier + ": Batch of [" +count+ "] events != from "+inboundEvents.size()+" that will be pushed to worker " + node.identifier);
                }
                vmsInternalChannels.transactionInputQueue().addAll(inboundEvents);
                LOGGER.log(DEBUG, "Number of inputs pending processing: "+vmsInternalChannels.transactionInputQueue().size());
            } catch(Exception e){
                if (e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading batch: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            } finally {
                inboundEvents.clear();
                LIST_BUFFER.add(inboundEvents);
            }
        }

        @Override
        public void failed(Throwable exc, Integer carryOn) {
            LOGGER.log(ERROR,me.identifier+": Error on reading VMS message from "+node.identifier+"\n"+exc);
            exc.printStackTrace(System.out);
            this.setUpNewRead();
        }
    }

    /**
     * On a connection attempt, it is unknown what is the type of node
     * attempting the connection. We find out after the first read.
     */
    private final class UnknownNodeReadCompletionHandler implements CompletionHandler<Integer, Void> {

        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;

        public UnknownNodeReadCompletionHandler(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            this.channel = channel;
            this.buffer = buffer;
        }

        @Override
        public void completed(Integer result, Void void_) {
            String remoteAddress = "";
            try {
                remoteAddress = channel.getRemoteAddress().toString();
            } catch (IOException ignored) { }
            if(result == 0){
                LOGGER.log(WARNING,me.identifier+": A node ("+remoteAddress+") is trying to connect with an empty message!");
                try { this.channel.close(); } catch (IOException ignored) {}
                return;
            } else if(result == -1){
                LOGGER.log(WARNING,me.identifier+": A node ("+remoteAddress+") died before sending the presentation message");
                try { this.channel.close(); } catch (IOException ignored) {}
                return;
            }
            // message identifier
            byte messageIdentifier = this.buffer.get(0);
            if(messageIdentifier != PRESENTATION){
                this.buffer.flip();
                String request = StandardCharsets.UTF_8.decode(this.buffer).toString();
                if(HttpUtils.isHttpClient(request)){
                    HttpReadCompletionHandler readCompletionHandler = new HttpReadCompletionHandler(
                            new ConnectionMetadata("http_client".hashCode(),
                                    ConnectionMetadata.NodeType.HTTP_CLIENT,
                                    this.channel),
                            this.buffer,
                            MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize),
                            httpHandler);
                    try { NetworkUtils.configure(this.channel, options.soBufferSize()); } catch (IOException ignored) { }
                    readCompletionHandler.parse(new HttpReadCompletionHandler.RequestTracking());
                } else {
                    LOGGER.log(WARNING, me.identifier + ": A node is trying to connect without a presentation message.\n"+request);
                    this.buffer.clear();
                    MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
                    try { this.channel.close(); } catch (IOException ignored) { }
                }
                return;
            }
            byte nodeTypeIdentifier = this.buffer.get(1);
            this.buffer.position(2);
            switch (nodeTypeIdentifier) {
                case (Presentation.SERVER_TYPE) -> this.processServerPresentation();
                case (Presentation.VMS_TYPE) -> this.processVmsPresentation();
                default -> this.processUnknownNodeType(nodeTypeIdentifier);
            }
        }

        private void processServerPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing presentation message from a node claiming to be a server");
            if(!leader.isActive()) {
                ConnectionFromLeaderProtocol connectionFromLeader = new ConnectionFromLeaderProtocol(this.channel, this.buffer);
                connectionFromLeader.processLeaderPresentation();
            } else {
                // discard include metadata bit
                this.buffer.get();
                ServerNode serverNode = Presentation.readServer(this.buffer);
                // known leader attempting additional connection?
                if(serverNode.asInetSocketAddress().equals(leader.asInetSocketAddress())) {
                    LOGGER.log(INFO, me.identifier + ": Leader requested an additional connection");
                    this.buffer.clear();
                    channel.read(buffer, 0, new LeaderReadCompletionHandler(new ConnectionMetadata(leader.hashCode(), ConnectionMetadata.NodeType.SERVER, channel), buffer));
                    generation = serverNode.generation;
                    System.out.println(STR."\{me.identifier} setting generation to \{generation} for serverNode gen=\{serverNode.generation} after leader connection");
                } else {
                    try {
                        LOGGER.log(WARNING,"Dropping a connection attempt from a node claiming to be leader");
                        this.channel.close();
                    } catch (Exception ignored) {}
                }
            }
        }

        private void processVmsPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing presentation message from a node claiming to be a VMS");

            // then it is a VMS intending to connect due to a data/event
            // that should be delivered to this vms
            VmsNode producerVms = Presentation.readVms(this.buffer, serdesProxy);
            LOGGER.log(INFO, me.identifier+": Producer VMS received:\n"+producerVms);
            this.buffer.clear();

            ConnectionMetadata connMetadata = new ConnectionMetadata(
                    producerVms.hashCode(),
                    ConnectionMetadata.NodeType.VMS,
                    this.channel
            );

            // what if a vms is both producer to and consumer from this vms?
            if(consumerVmsContainerMap.containsKey(producerVms)){
                LOGGER.log(WARNING,me.identifier+": The node "+producerVms.host+" "+producerVms.port+" already contains a connection as a consumer");
            }

            // just to keep track whether this
            if(producerConnectionMetadataMap.containsKey(producerVms.hashCode())) {
                LOGGER.log(INFO, me.identifier+": Setting up additional consumption from producer "+producerVms);
            } else {
                producerConnectionMetadataMap.put(producerVms.hashCode(), connMetadata);
                // setup event receiving from this vms
                LOGGER.log(INFO,me.identifier+": Setting up consumption from producer "+producerVms);
            }

            this.channel.read(this.buffer, 0, new VmsReadCompletionHandler(producerVms, connMetadata, this.buffer));
        }

        private void processUnknownNodeType(byte nodeTypeIdentifier) {
            LOGGER.log(WARNING,me.identifier+": Presentation message from unknown source:" + nodeTypeIdentifier);
            this.buffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
            try {
                this.channel.close();
            } catch (IOException ignored) { }
        }

        @Override
        public void failed(Throwable exc, Void void_) {
            LOGGER.log(WARNING,"Error on processing presentation message!");
        }
    }

    /**
     * Class is iteratively called by the socket pool threads.
     */
    private final class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {
        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {
            LOGGER.log(DEBUG,me.identifier+": An unknown host has started a connection attempt.");
            final ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize);
            try {
                NetworkUtils.configure(channel, options.soBufferSize);
                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                channel.read(buffer, null, new UnknownNodeReadCompletionHandler(channel, buffer));
            } catch(Exception e){
                LOGGER.log(ERROR,me.identifier+": Accept handler caught an exception:\n"+e);
                buffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(buffer);
            } finally {
                LOGGER.log(DEBUG,me.identifier+": Accept handler set up again for listening to new connections");
                // continue listening
                serverSocket.accept(null, this);
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            String message = exc.getMessage();
            boolean logError = true;
            if(message == null){
                if (exc.getCause() instanceof ClosedChannelException){
                    message = "Connection is closed";
                } else if ( exc instanceof AsynchronousCloseException || exc.getCause() instanceof AsynchronousCloseException) {
                    message = "Event handler has been stopped?";
                } else {
                    message = "No cause identified";
                }
                LOGGER.log(WARNING, me.identifier + ": Error on accepting connection: " + message);
            } else if(message.equalsIgnoreCase("Too many open files")){
                logError = false;
                System.out.println("Too many open files error was caught. Cannot log the error appropriately.");
            }

            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            } else if(logError) {
                LOGGER.log(WARNING,me.identifier+": Socket is not open anymore. Cannot set up accept again");
            }

        }
    }

    private final class ConnectionFromLeaderProtocol {
        private State state;
        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;
        public final CompletionHandler<Integer, Void> writeCompletionHandler;

        public ConnectionFromLeaderProtocol(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            this.state = State.PRESENTATION_RECEIVED;
            this.channel = channel;
            this.writeCompletionHandler = new WriteCompletionHandler();
            this.buffer = buffer;
        }

        private enum State {
            PRESENTATION_RECEIVED,
            PRESENTATION_PROCESSED,
            PRESENTATION_SENT
        }

        private final class WriteCompletionHandler implements CompletionHandler<Integer,Void> {

            @Override
            public void completed(Integer result, Void attachment) {
                state = State.PRESENTATION_SENT;
                LOGGER.log(INFO,me.identifier+": Message sent to Leader successfully = "+state);
                // set up leader worker
                leaderWorker = new LeaderWorker(me, leader,
                        leaderConnectionMetadata.channel,
                        MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize));
                LOGGER.log(INFO,me.identifier+": Leader worker set up");
                buffer.clear();
                channel.read(buffer, 0, new LeaderReadCompletionHandler(leaderConnectionMetadata, buffer) );
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                LOGGER.log(INFO,me.identifier+": Failed to send presentation to Leader");
                buffer.clear();
                if(!channel.isOpen()) {
                    leaderWorker.stop();
                    leader.off();
                }
                // else what to do try again? no, let the new leader connect
            }
        }

        public void processLeaderPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing the Leader presentation");
            boolean includeMetadata = this.buffer.get() == Presentation.YES;
            // leader has disconnected, or new leader
            leader = Presentation.readServer(this.buffer);
            generation = leader.generation;
            System.out.println(STR."\{me.identifier} setting generation to \{generation} for serverNode gen=\{leader.generation} after leader connection");

            // read queues leader is interested
            boolean hasQueuesToSubscribe = this.buffer.get() == Presentation.YES;
            if(hasQueuesToSubscribe){
                queuesLeaderSubscribesTo.addAll(Presentation.readQueuesToSubscribeTo(this.buffer, serdesProxy));
            }
            // only connects to all VMSs on first leader connection
            if(leaderConnectionMetadata != null) {
                // considering the leader has replicated the metadata before failing
                // so no need to send metadata again. but it may be necessary...
                // what if the tid and batch id is necessary. the replica may not be
                // sync with last leader...
                LOGGER.log(WARNING, me.identifier+": Updating leader connection metadata due to new connection");
            }
            leaderConnectionMetadata = new ConnectionMetadata(
                    leader.hashCode(),
                    ConnectionMetadata.NodeType.SERVER,
                    channel
            );
            leader.on();
            this.buffer.clear();
            if(includeMetadata) {
                String vmsDataSchemaStr = serdesProxy.serializeDataSchema(me.dataSchema);
                String vmsInputEventSchemaStr = serdesProxy.serializeEventSchema(me.inputEventSchema);
                String vmsOutputEventSchemaStr = serdesProxy.serializeEventSchema(me.outputEventSchema);
                Presentation.writeVms(this.buffer, me, me.identifier, me.batch, 0, me.previousBatch, vmsDataSchemaStr, vmsInputEventSchemaStr, vmsOutputEventSchemaStr);
                // the protocol requires the leader to wait for the metadata in order to start sending messages
            } else {
                Presentation.writeVms(this.buffer, me, me.identifier, me.batch, 0, me.previousBatch);
            }
            this.buffer.flip();
            this.state = State.PRESENTATION_PROCESSED;
            LOGGER.log(INFO,me.identifier+": Message successfully received from the Leader  = "+state);
            this.channel.write( this.buffer, null, this.writeCompletionHandler );
        }
    }

    private InboundEvent buildInboundEvent(TransactionEvent.Payload payload){
        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(payload.event());
        Object input = this.serdesProxy.deserialize(payload.payload(), clazz);
        Map<String, Long> precedenceMap = this.serdesProxy.deserializeDependenceMap(payload.precedenceMap());
        if(precedenceMap == null){
            throw new IllegalStateException("Precedence map is null.");
        }
        if(!precedenceMap.containsKey(this.me.identifier)){
            throw new IllegalStateException("Precedent tid of "+payload.tid()+" is unknown.");
        }
        this.tidToPrecedenceMap.put(payload.tid(), precedenceMap);
        return new InboundEvent( payload.tid(), precedenceMap.get(this.me.identifier),
                payload.batch(),
//                payload.generation(),
                0,
                payload.event(), clazz, input );
    }

    private static final ConcurrentLinkedDeque<List<InboundEvent>> LIST_BUFFER = new ConcurrentLinkedDeque<>();

    private final class LeaderReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;

        public LeaderReadCompletionHandler(ConnectionMetadata connectionMetadata, ByteBuffer readBuffer){
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = readBuffer;
            LIST_BUFFER.add(new ArrayList<>(1024));
        }

        @Override
        public void completed(Integer result, Integer startPos) {
//            System.out.println("Read from leader");
            if(result == -1){
                System.out.println("Leader has disconnected");
                LOGGER.log(INFO,me.identifier+": Leader has disconnected");
                leader.off();
                try {
                    this.connectionMetadata.channel.close();
                } catch (IOException e) {
                    e.printStackTrace(System.out);
                }
                return;
            }
            if(startPos == 0){
                // sets the position to 0 and sets the limit to the current position
                this.readBuffer.flip();
                LOGGER.log(DEBUG,me.identifier+": Leader has sent "+this.readBuffer.limit()+" bytes");
            }
            // guaranteed we always have at least one byte to read
            byte messageType = this.readBuffer.get();
            try {
                switch (messageType) {
                    case (BATCH_OF_EVENTS) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        this.processBatchOfEvents(this.readBuffer);
                    }
                    case (EVENT) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        this.processSingleEvent(readBuffer);
                    }
                    case (BATCH_COMMIT_INFO) -> {
                        if(this.readBuffer.remaining() < (BatchCommitInfo.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        // events of this batch from VMSs may arrive before the batch commit info
                        // it means this VMS is a terminal node for the batch
                        BatchCommitInfo.Payload bPayload = BatchCommitInfo.read(this.readBuffer);
                        this.processNewBatchInfo(bPayload);
                    }
                    case (BATCH_COMMIT_COMMAND) -> {
                        if(this.readBuffer.remaining() < (BatchCommitCommand.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        // a batch commit queue from next batch can arrive before this vms moves next? yes
                        BatchCommitCommand.Payload payload = BatchCommitCommand.read(this.readBuffer);
                        LOGGER.log(DEBUG, me.identifier + ": Batch (" + payload.batch() + ") commit command received from the leader");
                        this.processNewBatchCommand(payload);
                    }
                    case (TX_ABORT) -> {
                        if(this.readBuffer.remaining() < (TransactionAbort.SIZE - 1)) {
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        TransactionAbort.Payload txAbortPayload = TransactionAbort.read(this.readBuffer);
                        processAbort(txAbortPayload);
                    }
                    case (VMS_CRASH) -> {
                        VmsCrash.Payload vmsCrash = VmsCrash.read(this.readBuffer);
                        processVmsCrash(vmsCrash);
                    }
                    case (VMS_RECONNECTION) -> {
                        VmsReconnect.Payload vmsReconnect = VmsReconnect.read(this.readBuffer);
                        processVmsReconnection(vmsReconnect);
                    }
                    case (RESET_TO_COMMITTED) -> {
                        resetToCommittedState();
                        leaderWorker.queueMessage(ResetToCommittedAck.of(me.identifier));
                    }
                    case (CONSUMER_SET) -> {
                        try {
                            // System.out.println(STR."\{me.identifier} received consumer set");
                            Map<String, List<IdentifiableNode>> receivedConsumerVms = ConsumerSet.read(this.readBuffer, serdesProxy);
                            if (!receivedConsumerVms.isEmpty()) {
                                connectToReceivedConsumerSet(receivedConsumerVms);
                            } else {
                                LOGGER.log(WARNING, STR."consumerSet receoved in \{me.identifier} is empty");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    case (PRESENTATION) ->
                            LOGGER.log(WARNING, me.identifier + ": Presentation being sent again by the leader!?");
                    default ->
                            LOGGER.log(ERROR, me.identifier + ": Message type sent by the leader cannot be identified: " + messageType);
                }
            } catch (Exception e){
                System.out.println(STR."Error reading \{messageType} from leader in \{me.identifier}");
                e.printStackTrace();
            }

            if(this.readBuffer.hasRemaining()){
                this.completed(result, this.readBuffer.position());
            } else {
                this.setUpNewRead();
            }
        }

        private int getBufferSize() {
            int bufferSize = Integer.MAX_VALUE;
            // check if we can read an integer
            if(this.readBuffer.remaining() > Integer.BYTES) {
                // size of the batch
                bufferSize = this.readBuffer.getInt();
                // discard message type and size of batch from the total size since it has already been read
                bufferSize -= 1 + Integer.BYTES;
            }
            return bufferSize;
        }

        /**
         * This method should be called only when strictly necessary to complete a read
         * Otherwise there would be an overhead due to the many I/Os
         */
        private void fetchMoreBytes(Integer startPos) {
            this.readBuffer.position(startPos);
            this.readBuffer.compact();
            // get the rest of the batch
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void setUpNewRead() {
            this.readBuffer.clear();
            // set up another read for cases of bursts of data
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void processBatchOfEvents(ByteBuffer readBuffer) {
            List<InboundEvent> payloads = LIST_BUFFER.poll();
            if(payloads == null) payloads = new ArrayList<>(1024);
            /*
             * Given a new batch of events sent by the leader, the last message is the batch info
             */
            TransactionEvent.Payload payload;
            try {
                // to increase performance, one would buffer this buffer for processing and then read from another buffer
                int count = readBuffer.getInt();
                LOGGER.log(DEBUG,me.identifier + ": Batch of [" + count + "] events received from the leader");
                // extract events batched
                for (int i = 0; i < count; i++) {
                    payload = TransactionEvent.read(readBuffer);
                    LOGGER.log(DEBUG, me.identifier+": Processed TID "+payload.tid());
                    if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                        payloads.add(buildInboundEvent(payload));
                        continue;
                    }
                    LOGGER.log(WARNING,me.identifier + ": queue not identified for event received from the leader \n"+payload);
                }
                vmsInternalChannels.transactionInputQueue().addAll(payloads);
            } catch (Exception e){
                LOGGER.log(ERROR, me.identifier +": Error while processing a batch\n"+e);
                e.printStackTrace(System.out);
                if(e instanceof BufferUnderflowException) {
                    throw new RuntimeException(e);
                }
            } finally {
                payloads.clear();
                LIST_BUFFER.add(payloads);
            }
        }

        private void processSingleEvent(ByteBuffer readBuffer) {
            try {
                TransactionEvent.Payload payload = TransactionEvent.read(readBuffer);
                LOGGER.log(DEBUG,me.identifier + ": 1 event received from the leader \n"+payload);
                // send to scheduler.... drop if the event cannot be processed (not an input event in this vms)
                if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                    InboundEvent event = buildInboundEvent(payload);
                    vmsInternalChannels.transactionInputQueue().add(event);
                    return;
                }
                LOGGER.log(WARNING,me.identifier + ": queue not identified for event received from the leader \n"+payload);
            } catch (Exception e) {
                if(e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading event: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            }
        }

        private void processNewBatchInfo(BatchCommitInfo.Payload batchCommitInfo)
        {
            BatchContext batchContext = BatchContext.build(batchCommitInfo);

            // System.out.println(STR."\{me.identifier} received BATCH COMMIT INFO \{batchCommitInfo}");

            var batch = batchCommitInfo.batch();

            // check if batch context needs updating
            // are the aborted TIDs registered
            var abortedTIDs = batchAbortedTIDs.get(batch);
            if (abortedTIDs != null) {
                for (var abortedTID : abortedTIDs) {
                    if (!batchContext.abortedTIDs.contains(abortedTID)) {
                        batchContext.numberOfTIDsBatch -= 1;
                        batchContext.abortedTIDs.add(abortedTID);
                    }
                }
            }

            batchContextMap.put(batch, batchContext);
            // System.out.println(STR."\{me.identifier} has put batchContext for batch \{batch}");

            // if it has been completed but not moved to status, then should send
            if(trackingBatchMap.containsKey(batch))
            {
                var trackedBatch = trackingBatchMap.get(batchCommitInfo.batch());
                System.out.println(STR."\{me.identifier} has processed \{trackedBatch.numberTIDsExecuted} for batch \{batch}");

                if (trackedBatch.numberTIDsExecuted != batchCommitInfo.numberOfTIDsBatch())
                    return;

                LOGGER.log(INFO,me.identifier+": Requesting leader worker to send batch ("+batchCommitInfo.batch()+") complete (LATE)");
                leaderWorker.queueMessage(BatchComplete.of(batchCommitInfo.batch(), me.identifier));
                return;
            }

            // System.out.println(STR."\{me.identifier} has not tracked any events for batch \{batch}");
        }

        /**
         * Context of execution of this method:
         * This is not a terminal node in this batch
         */
        private void processNewBatchCommand(BatchCommitCommand.Payload batchCommitCommand)
        {
            // committing output events sent from the VMS

            BatchContext batchContext = BatchContext.build(batchCommitCommand);
            batchContextMap.put(batchCommitCommand.batch(), batchContext);
            BatchMetadata batchMetadata = trackingBatchMap.get(batchCommitCommand.batch());
            if(batchMetadata == null){
                LOGGER.log(WARNING,me.identifier+": Cannot find tracking of batch "+ batchCommitCommand.batch());
                return;
            }
            if(batchContext.numberOfTIDsBatch != batchMetadata.numberTIDsExecuted) {
                LOGGER.log(WARNING,me.identifier+": Batch "+ batchCommitCommand.batch()+" has not yet finished!");
                return;
            }
            LOGGER.log(DEBUG, me.identifier + ": All TIDs for the batch " + batchCommitCommand.batch() + " have been executed");
            batchContext.setStatus(BatchContext.BATCH_COMPLETED);
            loggingHandler.commit(batchCommitCommand.batch());

            if(options.checkpointing()){
                LOGGER.log(INFO, me.identifier + ": Requesting checkpoint for batch " + batchCommitCommand.batch());

                // committing the actual data snapshot and the commitInfo (metadata) about the snapshot
                // argue that both of these should be atomic
                submitBackgroundTask(()->{
                    // System.out.println(STR."checkpointing batch \{batchCommitCommand.batch()} in \{me.identifier}");
                    checkpoint(batchCommitCommand.batch(), batchMetadata.maxTidExecuted);

                    // log commit info only if snapshot was modified
                    if (trackingBatchMap.get(batchCommitCommand.batch()).numberTIDsExecuted > 0) {

//                        System.out.println(STR."\{me.identifier} executed " +
//                                           STR."\{trackingBatchMap.get(batchCommitCommand.batch()).numberTIDsExecuted} " +
//                                           STR."tids in batch \{batchCommitCommand.batch()}");

                        commitInfo.put(batchCommitCommand.batch(), trackingBatchMap.get(batchCommitCommand.batch()).maxTidExecuted);
                        // System.out.println(STR."Batch \{batchCommitCommand.batch()} committed in \{me.identifier}");
                    }
                });
            }
        }

        @Override
        public void failed(Throwable exc, Integer carryOn) {
            LOGGER.log(ERROR,me.identifier+": Message could not be processed: "+exc);
            exc.printStackTrace(System.out);
            this.setUpNewRead();
        }
    }

    public void close() {
        this.stop();
        for(var consumer : this.consumerVmsContainerMap.entrySet()){
            consumer.getValue().stop();
        }
        try {
            this.serverSocket.close();
        } catch (IOException ignored){ }
    }

}