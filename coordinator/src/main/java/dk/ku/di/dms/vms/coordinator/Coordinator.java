package dk.ku.di.dms.vms.coordinator;

import dk.ku.di.dms.vms.coordinator.batch.BatchAlgo;
import dk.ku.di.dms.vms.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.batch.TxWorkerProcessedCrash;
import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.options.VmsWorkerOptions;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionWorker;
import dk.ku.di.dms.vms.coordinator.vms.IVmsWorker;
import dk.ku.di.dms.vms.coordinator.vms.VmsWorker;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import dk.ku.di.dms.vms.modb.common.logging.LongPairStore;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.control.*;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.*;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.web_common.HttpUtils;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import dk.ku.di.dms.vms.web_common.ModbHttpServer;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.JdkAsyncChannel;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.SERVER_TYPE;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.SERVER;
import static java.lang.System.Logger.Level.*;

/**
 * Also known as the "Leader"
 * Class that encapsulates all logic related to issuing of
 * transactions, batch commits, transaction aborts, ...
 */
public final class Coordinator extends ModbHttpServer {

    private static final System.Logger LOGGER = System.getLogger(Coordinator.class.getName());
    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final CoordinatorOptions options;

    // this server socket
    private final AsynchronousServerSocketChannel serverSocket;

    // group for channels
    private final AsynchronousChannelGroup group;

    // even though we can start with a known number of servers, their payload may have changed after a crash
    private final Map<Integer, ServerNode> servers;

    // for server nodes
    private final Map<Integer, LockConnectionMetadata> serverConnectionMetadataMap;

    /* VMS data structures **/

    /**
     * received from program start
     * also called known VMSs
     */
    private final Map<String, IdentifiableNode> starterVMSs;

    /**
     * Those received from program start + those that joined later
     * shared with vms workers
     */
    private final Map<String, VmsNode> vmsMetadataMap;

    // the identification of this server
    private final ServerNode me;

    /*
     * the offset of the pending batch commit (always < batchOffset)
     * volatile because it is accessed by http threads from driver requests
     */
    private volatile long batchOffsetPendingCommit;

    // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    // transaction requests coming from the http event loop
    private final List<ConcurrentLinkedDeque<TransactionInput>> transactionInputDeques;
    private List<ConcurrentLinkedDeque<Object>> transactionWorkerMessageQueues;

    // transaction definitions coming from the http event loop
    private final Map<String, TransactionDAG> transactionMap;

    // map input events to vms identifiers
    private final Map<String, List<String>> eventToConsumersMap;
    private final Map<String, List<String>> consumerToEventsMap;

    // serialization and deserialization of complex objects
    private final IVmsSerdesProxy serdesProxy;

    private final BlockingQueue<Object> coordinatorQueue;

    private final Map<String, IVmsWorker> vmsWorkerContainerMap;

    private final List<Tuple<TransactionWorker, Thread>> transactionWorkers;

    private final Consumer<TransactionInput> transactionInputConsumer;
    private boolean coordinatorIsRecovering;
    private boolean processingCrash;

    private final IHttpHandler httpHandler;

    private ILoggingHandler loggingHandler;

    private Set<String> disallowedTransactions;
    private Set<String> offlineVMSes;
    private Map<Long, Set<TransactionEvent.Payload>> batchAbortedTxInputs;
    private Map<Long, Set<String>> ongoingAbortMissingVmsAck;
    private Set<Long> pendingAborts;
    private Map<String, Set<String>> ongoingCrashMissingVmsAck;
    private Map<String, Set<String>> ongoingReconnectionMissingVmsAck;
    private Set<String> resetToCommittedAck;

    public static Coordinator build(Properties properties, Map<String, IdentifiableNode> startersVMSs,
                                    Map<String, TransactionDAG> transactionMap, Function<Coordinator, IHttpHandler> httpHandlerSupplier){

        int tcpPort = Integer.parseInt( properties.getProperty("tcp_port") );
        ServerNode serverIdentifier = new ServerNode( "0.0.0.0", tcpPort );

        Map<Integer, ServerNode> serverMap = new HashMap<>();
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        // recovery
        var recoveryEnabled = Boolean.parseBoolean(properties.getProperty("recoverable"));

        // network
        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        int soBufferSize = Integer.parseInt( properties.getProperty("so_buffer_size") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );
        int networkSendTimeout = Integer.parseInt( properties.getProperty("network_send_timeout") );
        int definiteBufferSize = networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize;

        // batch generation
        int batchWindow = Integer.parseInt( properties.getProperty("batch_window_ms") );
        int batchMaxTransactions = Integer.parseInt( properties.getProperty("num_max_transactions_batch") );
        int numTransactionWorkers = Integer.parseInt( properties.getProperty("num_transaction_workers") );

        // vms worker config
        int numWorkersPerVms = Integer.parseInt( properties.getProperty("num_vms_workers") );
        int numQueuesVmsWorker = Integer.parseInt( properties.getProperty("num_queues_vms_worker"));
        int maxSleep = Integer.parseInt( properties.getProperty("max_sleep") );

        // logging
        boolean logging = Boolean.parseBoolean( properties.getProperty("logging") );

        return Coordinator.build(
                serverMap,
                startersVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions()
                        .withNetworkBufferSize(definiteBufferSize)
                        .withSoBufferSize(soBufferSize)
                        .withNetworkThreadPoolSize(groupPoolSize)
                        .withNetworkSendTimeout(networkSendTimeout)
                        .withBatchWindow(batchWindow)
                        .withMaxTransactionsPerBatch(batchMaxTransactions)
                        .withNumTransactionWorkers(numTransactionWorkers)
                        .withNumWorkersPerVms(numWorkersPerVms)
                        .withNumQueuesVmsWorker(numQueuesVmsWorker)
                        .withMaxVmsWorkerSleep(maxSleep)
                        .withLogging(logging)
                        .withRecoveryEnabled(recoveryEnabled)
                ,
                httpHandlerSupplier,
                serdes
        );
    }

    public static Coordinator build(// obtained from leader election or passed by parameter on setup
                                    Map<Integer, ServerNode> servers,
                                    // passed by parameter
                                    Map<String, IdentifiableNode> startersVMSs,
                                    Map<String, TransactionDAG> transactionMap,
                                    ServerNode me,
                                    // coordinator configuration
                                    CoordinatorOptions options,
                                    Function<Coordinator, IHttpHandler> httpHandlerSupplier,
                                    IVmsSerdesProxy serdesProxy) {
        return new Coordinator(servers == null ? new ConcurrentHashMap<>() : servers,
                new HashMap<>(), startersVMSs, transactionMap,
                me, options, httpHandlerSupplier, serdesProxy);
    }

    private Coordinator(Map<Integer, ServerNode> servers,
                        Map<Integer, LockConnectionMetadata> serverConnectionMetadataMap,
                        Map<String, IdentifiableNode> startersVMSs,
                        Map<String, TransactionDAG> transactionMap,
                        ServerNode me,
                        CoordinatorOptions options,
                        Function<Coordinator, IHttpHandler> httpHandlerSupplier,
                        IVmsSerdesProxy serdesProxy) {
        super();

        // coordinator options
        this.options = options;

        var crashOccurred = true;
        var recoveryEnabled = options.getRecoveryEnabled();
        System.out.println(STR."crashOccurred=\{crashOccurred} && recoveryEnabled=\{recoveryEnabled}");

        coordinatorIsRecovering = crashOccurred && recoveryEnabled;
        var truncate = coordinatorIsRecovering ? false : true;

        processingCrash = false;

        var logIdentifier = "coordinator_out";
        this.loggingHandler = LoggingHandlerBuilder.build(logIdentifier, serdesProxy, this.options.getNetworkBufferSize(), truncate);
        this.disallowedTransactions = new HashSet<>();
        this.offlineVMSes = new HashSet<>();
        this.ongoingAbortMissingVmsAck = new HashMap<>();
        this.batchAbortedTxInputs = new HashMap<>();
        this.pendingAborts = new HashSet<>();
        this.ongoingCrashMissingVmsAck = new HashMap<>();
        this.ongoingReconnectionMissingVmsAck = new HashMap<>();
        this.resetToCommittedAck = new HashSet<>();

        try {
            if (options.getNetworkThreadPoolSize() > 0) {
                this.group = AsynchronousChannelGroup.withThreadPool(Executors.newWorkStealingPool(options.getNetworkThreadPoolSize()));
                this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
            } else {
            /* may lead to better performance than default group
            this.group = AsynchronousChannelGroup.withThreadPool(ForkJoinPool.commonPool());
            this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
             */
                this.group = null;
                this.serverSocket = AsynchronousServerSocketChannel.open();
            }

            // network and executor
            this.serverSocket.bind(me.asInetSocketAddress());
        } catch (Exception e){
            throw new RuntimeException(e);
        }

        this.starterVMSs = startersVMSs;
        this.vmsMetadataMap = new ConcurrentHashMap<>();

        // might come filled from election process
        this.servers = servers;
        this.serverConnectionMetadataMap = serverConnectionMetadataMap;
        this.me = me;

        // infra
        this.serdesProxy = serdesProxy;

        // shared data structure with http handler
        this.transactionInputDeques = new ArrayList<>();
        this.transactionWorkerMessageQueues = new ArrayList<>();
        for(int i = 0; i < options.getNumTransactionWorkers(); i++){
            this.transactionInputDeques.add(new ConcurrentLinkedDeque<>());
            this.transactionWorkerMessageQueues.add(new ConcurrentLinkedDeque<>());
        }

        if(options.getNumTransactionWorkers() == 1){
            var inputQueue = this.transactionInputDeques.getFirst();
            this.transactionInputConsumer = inputQueue::offerLast;
        } else {
            transactionInputConsumer = transactionInput -> {
                int idx = ThreadLocalRandom.current().nextInt(0, this.options.getNumTransactionWorkers());
                this.transactionInputDeques.get(idx).offerLast(transactionInput);
            };
        }

        // in production, it requires receiving new transaction definitions
        this.transactionMap = transactionMap;

        // define event to input consumers
        this.eventToConsumersMap = new HashMap<>();
        consumerToEventsMap = new HashMap<>();
        for(var entry : this.transactionMap.entrySet()){
            for(var input : entry.getValue().inputEvents.entrySet()){
                var eventIdentifier = input.getValue();
                // extra func
                eventToConsumersMap.putIfAbsent(eventIdentifier.name, new ArrayList<>());
                eventToConsumersMap.get(eventIdentifier.name).add(eventIdentifier.targetVms);
//                System.out.println(STR."Added consumer \{eventIdentifier.targetVms} to consumers in map from event \{eventIdentifier.name}");

                consumerToEventsMap.putIfAbsent(eventIdentifier.targetVms, new ArrayList<>());
                consumerToEventsMap.get(eventIdentifier.targetVms).add(eventIdentifier.name);
//                System.out.println(STR."Added event \{eventIdentifier.name} to events in map from consumer \{eventIdentifier.targetVms}");
            }
        }


        // to hold actions spawned by events received by different VMSs
        this.coordinatorQueue = new LinkedBlockingQueue<>();

        // to store information about a batch
        this.batchContextMap = new ConcurrentHashMap<>();

        this.vmsWorkerContainerMap = new HashMap<>();
        this.transactionWorkers = new ArrayList<>();
        this.httpHandler = httpHandlerSupplier.apply(this);
    }

    private final Map<String, VmsNode[]> vmsIdentifiersPerDAG = new HashMap<>();

    /**
     * This method contains the event loop that contains the main functions of a leader/coordinator
     * What happens if two nodes declare themselves as leaders? We need some way to let it know
     * OK (b) Batch management
     * designing leader mode first
     * design follower mode in another class to avoid convoluted code.
     * Going for a different socket to allow for heterogeneous ways for a client to connect with the servers e.g., http.
     * It is also good to better separate resources, so VMSs and followers do not share resources with external clients
     */
    @Override
    public void run() {

        // setup asynchronous listener for new connections
        this.serverSocket.accept(null, new AcceptCompletionHandler());

        // connect to all virtual microservices
        this.setupStarterVMSs();
        this.preprocessDAGs();

        // sleep
        System.out.println("Sleeping for 1 second...");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {}

        /////////////////////////////////////////////
        /////////////////////////////////////////////
        if (coordinatorIsRecovering)
        {
            try {
                // get the latest committed info
                var latestCommitInfo = loggingHandler.latestCommit();
                var batchOffset = latestCommitInfo[0];
                var tid = latestCommitInfo[1];
                var resetToCommittedStateMessage = ResetToCommittedState.of();

                // NEED ACKs
                for (var vms: vmsMetadataMap.values())
                {
                    this.vmsWorkerContainerMap.get(vms.identifier).queueMessage(resetToCommittedStateMessage);
                }

                System.out.println(STR."Coordinator start latest tid and bid are \{tid} and \{batchOffset}");

                batchOffsetPendingCommit = batchOffset+1;

                this.setUpTransactionWorkers(tid+1, batchOffset+1);
                }
            catch (Exception e) {
                batchOffsetPendingCommit = 1;
                e.printStackTrace();
                this.setUpTransactionWorkers();
            }
        } else {
            batchOffsetPendingCommit = 1;
            this.setUpTransactionWorkers();
        }
        /////////////////////////////////////////////
        /////////////////////////////////////////////

        // event loop
        try {
            Object message;
            do {
                // tends to be faster than blocking
                while ((message = this.coordinatorQueue.poll(250, TimeUnit.MILLISECONDS)) != null) {
                    this.processVmsMessage(message);
                }
            } while (this.isRunning());
        } catch (Exception e){
            e.printStackTrace(System.out);
        }

        this.close();
        LOGGER.log(INFO,"Leader: Finished execution.");
    }

    private Map<String, TransactionWorker.PrecedenceInfo> buildStarterPrecedenceMap() {
        return buildStarterPrecedenceMap(false);
    }
    private Map<String, TransactionWorker.PrecedenceInfo> buildStarterPrecedenceMap(boolean loadFromLogs)
    {
        // System.out.println(STR."Building starter precedence map from logs=\{loadFromLogs}");
        // precedence info for each VMS
        Map<String, TransactionWorker.PrecedenceInfo> precedenceMap = new HashMap<>();

        // all event types. These are transaction inputs
        Set<String> inputEvents = eventToConsumersMap.keySet();


        // System.out.println(STR."Building starter precedence map, eventTypes count: \{inputEvents.size()}");

        // the tid and bid a given event type was seen at most recently
        Map<String, long[]> eventTypeLatestAppearance = new HashMap<>();
        for (var eventType : inputEvents) {
            eventTypeLatestAppearance.put(eventType, new long[] {0, 0});
        }

        // populate eventTypeLatestAppearance from logs
        if (loadFromLogs) {
            try {
                var loggedAppearances = loggingHandler.getLatestAppearanceOfEventTypes(inputEvents);
                for (var entry : loggedAppearances.entrySet()) {
//                    System.out.println(STR."LATEST APPEARANCE FOR \{entry.getKey()} is tid=\{entry.getValue()[0]} and bid=\{entry.getValue()[1]}");
                    eventTypeLatestAppearance.put(entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // get the dag for each eventType
        for (var eventType : inputEvents)
        {
            System.out.println(STR."Checking nodes in dag with input event: \{eventType}");

            // only one???
            var dagsWithEventTypeAsInput = transactionMap.values().stream().filter(d-> d.inputEvents.containsKey(eventType)).toList();
            var dag = dagsWithEventTypeAsInput.stream().findFirst().get();

            if (dag == null) {
                System.out.println(STR."dag is null for: \{eventType}");
                continue;
            };

            var nodes = dag.getNodes();
            System.out.println(STR."\{nodes.size()} nodes in dag with input event: \{eventType}");

            for (var node : nodes)
            {
                // should in principle never be triggered
                if (!vmsMetadataMap.containsKey(node)) continue;


                long[] latestInfo = eventTypeLatestAppearance.get(eventType);
                long lastTid = latestInfo[0];
                long lastBatch = latestInfo[1];
                // System.out.println(STR."latest tid and bid for \{node} in dag with eventType \{eventType} are \{lastTid} and \{lastBatch}");
                var precedenceInfoVms = new TransactionWorker.PrecedenceInfo(lastTid, lastBatch, 0);
                // System.out.println(STR."precedenceInfoVms for vms \{node}: \{precedenceInfoVms}");

                if (!precedenceMap.containsKey(node)) {
                    precedenceMap.put(node, precedenceInfoVms);
                    continue;
                }

                var oldPrecedenceInfoNode = precedenceMap.get(node);
                if (oldPrecedenceInfoNode.lastTid() < precedenceInfoVms.lastTid()) {
                    precedenceMap.put(node, precedenceInfoVms);
                }
            }
        }
//        for (var entry : precedenceMap.entrySet()) {
//            System.out.println(STR."precedenceMap for \{entry.getKey()} is \{entry.getValue().toString()}");
//        }

        return precedenceMap;
    }

    // non recovering
    private void setUpTransactionWorkers() {
        setUpTransactionWorkers(1, 1);
    }
    private void setUpTransactionWorkers(long startingTid, long startingBatchOffset) {
        int numWorkers = this.options.getNumTransactionWorkers();
        System.out.println(STR."start setup \{numWorkers} transaction workers");

        var firstPrecedenceInputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();
        var precedenceMapInputQueue = firstPrecedenceInputQueue;
        ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>> precedenceMapOutputQueue;


        var starterPrecedenceMap = startingTid == 1 ? buildStarterPrecedenceMap() : buildStarterPrecedenceMap(true);

        // each worker should have different starting TIDs and BIDs
        firstPrecedenceInputQueue.add(starterPrecedenceMap);

        long initTid = startingTid;
        int idx = 1;
        do {
            if(idx < numWorkers){
                precedenceMapOutputQueue = new ConcurrentLinkedDeque<>();
            } else {
                // complete the ring
                precedenceMapOutputQueue = firstPrecedenceInputQueue;
            }
            var txInputQueue = this.transactionInputDeques.get(idx-1);
            var messageQueue = this.transactionWorkerMessageQueues.get(idx-1);

            TransactionWorker txWorker = TransactionWorker.build(idx, txInputQueue, messageQueue,
                    initTid,
                    this.options.getMaxTransactionsPerBatch(), this.options.getBatchWindow(),
                    numWorkers, precedenceMapInputQueue, precedenceMapOutputQueue, this.transactionMap,
                    this.vmsIdentifiersPerDAG, this.vmsWorkerContainerMap, this.coordinatorQueue, this.serdesProxy,
                    startingBatchOffset+idx-1);
            Thread txWorkerThread = Thread.ofPlatform()
                    .name("tx-worker-"+idx)
                    .inheritInheritableThreadLocals(false)
                    .unstarted(txWorker);

            initTid = initTid + this.options.getMaxTransactionsPerBatch();
            precedenceMapInputQueue = precedenceMapOutputQueue;
            idx++;
            this.transactionWorkers.add(Tuple.of( txWorker, txWorkerThread ));
        } while (idx <= numWorkers);

        // start them all
        for(var txWorker : this.transactionWorkers){
            txWorker.t2().start();
        }
        System.out.println(STR."finished setUpTransactionWorkers");
    }
    private void destroyTransactionWorkers()
    {
        System.out.println(STR."start destroyTransactionWorkers");
        for (var tuple : transactionWorkers) {
            tuple.t1().stop();
        }
        for (var tuple : transactionWorkers) {
            tuple.t2().interrupt();
        }
        for (var tuple : transactionWorkers) {
            try {
                tuple.t2().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (var q : transactionInputDeques) {
            q.clear();
        }
        for (var q : transactionWorkerMessageQueues) {
            q.clear();
        }
        transactionWorkers.clear();
        System.out.println(STR."finished destroyTransactionWorkers");
    }


    /**
     * Process all VMS_IDENTIFIER first before submitting transactions
     */
    private void waitForAllStarterVMSs() {
        LOGGER.log(DEBUG,"Leader: Waiting for all starter VMSs to connect");
        do {
            this.processMessagesSentByVmsWorkers();
        } while (this.vmsMetadataMap.size() < this.starterVMSs.size());
    }

    private void preprocessDAGs() {
        int numWorkersPerVms = this.options.getNumWorkersPerVms();
        LOGGER.log(DEBUG,"Leader: Preprocessing transaction DAGs");
        Set<String> inputVMSsSeen = new HashSet<>();
        // build list of VmsIdentifier per transaction DAG
        for(var dag : this.transactionMap.entrySet()){
            this.vmsIdentifiersPerDAG.put(dag.getKey(), BatchAlgo.buildTransactionDagVmsList( dag.getValue(), this.vmsMetadataMap ));

            // set up additional connection with vms that start transactions
            for(var inputVms : dag.getValue().inputEvents.entrySet()){
                var vmsNode = this.vmsMetadataMap.get(inputVms.getValue().targetVms);
                if(vmsNode == null || inputVMSsSeen.contains(vmsNode.identifier)){
                    continue;
                }
                inputVMSsSeen.add(vmsNode.identifier);
                for(int i = 1; i < numWorkersPerVms; i++){
                    if(this.vmsWorkerContainerMap.containsKey(inputVms.getValue().targetVms)) {
                        try {
                            VmsWorker newWorker = VmsWorker.build(this.me, vmsNode, this.coordinatorQueue,
                                    () -> JdkAsyncChannel.create(this.group),
                                    new VmsWorkerOptions(
                                            true,
                                            this.options.logging(),
                                            this.options.getMaxVmsWorkerSleep(),
                                            this.options.getNetworkBufferSize(),
                                            this.options.getNetworkSendTimeout(),
                                            this.options.getNumQueuesVmsWorker(),
                                            false),
                                    this.serdesProxy);
                            if(this.vmsWorkerContainerMap.get(inputVms.getValue().targetVms) instanceof VmsWorkerContainer o){
                                o.addWorker(newWorker);
                                Thread vmsWorkerThread = Thread.ofPlatform().factory().newThread(newWorker);
                                vmsWorkerThread.setName("vms-worker-" + vmsNode.identifier + "-" + (i));
                                vmsWorkerThread.start();
                            } else {
                                LOGGER.log(WARNING, "Leader: "+vmsNode.identifier+" type is unknown: "+this.vmsWorkerContainerMap.get(inputVms.getValue().targetVms).getClass().getName()+ ". Cannot spawn the worker thread!");
                            }
                        } catch (Exception e) {
                            LOGGER.log(WARNING, "Leader: Could not connect to VMS "+vmsNode.identifier, e);
                        }
                    }
                }
            }
        }
    }

    public void clearTransactionInputs() {
        for(var inputDeque : this.transactionInputDeques){
            inputDeque.clear();
        }
    }

    /**
     * A container of vms workers for the same VMS
     * Make a circular buffer. so events are spread among the workers (i.e., channels)
     */
    static final class VmsWorkerContainer implements IVmsWorker {
        private final int numVmsWorkers;
        private final VmsWorker[] vmsWorkers;
        private int nextPos;
        private final Consumer<TransactionEvent.PayloadRaw> queueFunc;
        private final Consumer<TransactionEvent.PayloadRaw> log;

        VmsWorkerContainer(VmsWorker initialVmsWorker, int numVmsWorkers,
                           Consumer<TransactionEvent.PayloadRaw> log) {
            this.numVmsWorkers = numVmsWorkers;
            this.vmsWorkers = new VmsWorker[numVmsWorkers];
            this.vmsWorkers[0] = initialVmsWorker;
            this.nextPos = 1;
            this.log = log;
            if(numVmsWorkers > 1){
                this.queueFunc = (payload) -> {
                    int pos = ThreadLocalRandom.current().nextInt(0, this.numVmsWorkers);
                    this.vmsWorkers[pos].queueTransactionEvent(payload);
                };
            } else {
                this.queueFunc = (payload) -> {
                    this.vmsWorkers[0].queueTransactionEvent(payload);
                };
            }
        }

        public void addWorker(VmsWorker vmsWorker) {
            this.vmsWorkers[this.nextPos] = vmsWorker;
            this.nextPos++;
        }

        @Override
        public void queueMessage(Object object){
            // always goes to first
            this.vmsWorkers[0].queueMessage(object);
        }

        // Assumption: This will always be called before the batch context is processed
        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payload)
        {
            log.accept(payload);
            this.queueFunc.accept(payload);
        }
        @Override
        public void requeueTransactionEvent(TransactionEvent.PayloadRaw payload) {
            this.queueFunc.accept(payload);
        }
    }

    /**
     * After a leader election, it makes more sense that
     * the leader connects to all known virtual microservices.
     * No need to track the thread created because they will be
     * later mapped to the respective vms identifier by the thread
     */
    private void setupStarterVMSs() {
        var inputsVMSs = new HashSet<String>();
        for(var entry : this.transactionMap.entrySet()){
            for(var input : entry.getValue().inputEvents.entrySet()){
                var eventIdentifier = input.getValue();
                inputsVMSs.add(eventIdentifier.targetVms);
            }
        }
        try {
            for (IdentifiableNode vmsNode : this.starterVMSs.values()) {
                // is this a VMS that receives transaction input?
                boolean active = inputsVMSs.contains(vmsNode.identifier);
                // coordinator will later keep track of this thread when
                // the connection with the VMS is fully established
                initVmsWorker(vmsNode, active);
            }
        } catch (Exception e){
            LOGGER.log(ERROR, "It was not possible to connect to one of the starter VMSs: " + e.getMessage());
            e.printStackTrace(System.out);
            throw new RuntimeException(e);
        }
        this.waitForAllStarterVMSs();
    }

    private void initVmsWorker(IdentifiableNode vmsNode, boolean active) throws IOException
    {
        // System.out.println(STR."coordinator creates worker for \{vmsNode.identifier}");
        VmsWorker vmsWorker = VmsWorker.build(this.me, vmsNode, this.coordinatorQueue,
                () -> JdkAsyncChannel.create(this.group),
                new VmsWorkerOptions(
                        active,
                        this.options.logging(),
                        this.options.getMaxVmsWorkerSleep(),
                        this.options.getNetworkBufferSize(),
                        this.options.getNetworkSendTimeout(),
                        this.options.getNumQueuesVmsWorker(),
                        true),
                this.serdesProxy);
        // virtual thread leads to performance degradation
        Thread vmsWorkerThread = Thread.ofPlatform().factory().newThread(vmsWorker);
        vmsWorkerThread.setName("vms-worker-" + vmsNode.identifier + "-0");
        this.vmsWorkerContainerMap.put(vmsNode.identifier,
                new VmsWorkerContainer(vmsWorker, this.options.getNumWorkersPerVms(),
                        this.loggingHandler::log)
        );
        vmsWorkerThread.start();
    }
    /**
     * Match output of a vms with the input of another
     * for each vms input event (not generated by the coordinator),
     * find the vms that generated the output
     */
    private List<IdentifiableNode> findConsumerVMSs(String outputEvent){
        List<IdentifiableNode> list = new ArrayList<>();
        // can be the leader or a vms
        for(VmsNode vms : this.vmsMetadataMap.values()){
            if(vms.inputEventSchema.containsKey(outputEvent)){
                list.add(vms.asIdentifiableNode());
            }
        }
        // assumed to be terminal? maybe yes.
        // vms is already connected to leader, no need to return coordinator
        return list;
    }

    private void close(){
        for(var txWorker : this.transactionWorkers){
            txWorker.t1.stop();
        }
        for(var vmsWorker : this.vmsWorkerContainerMap.entrySet()){
            vmsWorker.getValue().stop();
        }
        try { this.serverSocket.close(); } catch (IOException ignored) {}
    }

    /**
     * This is where I define whether the connection must be kept alive
     * Depending on the nature of the request:
     * <a href="https://www.baeldung.com/java-nio2-async-socket-channel">...</a>
     * The first read must be a presentation message, informing what is this server (follower or VMS)
     */
    private final class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {
        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {
            ByteBuffer buffer = null;
            try {
                // System.out.println("Accepting connection");
                NetworkUtils.configure(channel, options.getSoBufferSize());

                // right now I cannot discern whether it is a VMS or follower. perhaps I can keep alive channels from leader election?
                buffer = MemoryManager.getTemporaryDirectBuffer(options.getNetworkBufferSize());

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                // this will be handled by another thread in the group
                channel.read(buffer, buffer, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, ByteBuffer buffer) {
                        // set this thread free. release the thread that belongs to the channel group
                        processReadAfterAcceptConnection(channel, buffer);
                    }
                    @Override
                    public void failed(Throwable exc, ByteBuffer buffer) {
                        MemoryManager.releaseTemporaryDirectBuffer(buffer);
                    }
                });
            } catch (Exception e) {
                LOGGER.log(WARNING,"Leader: Error on accepting connection on " + me);
                if (buffer != null) {
                    MemoryManager.releaseTemporaryDirectBuffer(buffer);
                }
            } finally {
                // continue listening
                if (serverSocket.isOpen()) {
                    serverSocket.accept(null, this);
                }
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (serverSocket.isOpen()) {
                serverSocket.accept(null, this);
            }
        }

        /**
         * Process Accept connection request
         * Task for informing the server running for leader that a leader is already established
         * We would no longer need to establish connection in case the {@link dk.ku.di.dms.vms.coordinator.election.ElectionWorker}
         * maintains the connections.
         */
        private void processReadAfterAcceptConnection(AsynchronousSocketChannel channel, ByteBuffer buffer)
        {
            // message identifier
            byte messageIdentifier = buffer.get(0);

            if(messageIdentifier == VOTE_REQUEST || messageIdentifier == VOTE_RESPONSE){
                // so I am leader, and I respond with a leader request to this new node
                // taskExecutor.submit( new ElectionWorker.WriteTask( LEADER_REQUEST, server ) );
                // would be better to maintain the connection open.....
                buffer.clear();

                if(channel.isOpen()) {
                    LeaderRequest.write(buffer, me);
                    buffer.flip();
                    try (channel) {
                        channel.write(buffer); // write and forget
                    } catch (IOException ignored) {
                    } finally {
                        MemoryManager.releaseTemporaryDirectBuffer(buffer);
                    }
                }
                return;
            }

            if(messageIdentifier == LEADER_REQUEST){
                // buggy node intending to pose as leader...
                LOGGER.log(WARNING,"Leader: A node is trying to present itself as leader!");
                try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch(Exception ignored){}
                return;
            }

            if (messageIdentifier == VMS_RECONNECTION)
            {
                buffer.position(1);
                var node = IdentifiableNode.read(buffer); // this can potentially be hacked

                System.out.println(STR."the coordinator was PINGED by \{node.identifier}");

                boolean active = consumerToEventsMap.get(node.identifier).size() > 0;
                try {
                    initVmsWorker(node, active);
                    // rest of reconnection happens in vmsIdentifier
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return;
            }

            // if it is not a presentation, drop connection
            if(messageIdentifier != PRESENTATION){
                buffer.flip();
                String request = StandardCharsets.UTF_8.decode(buffer).toString();
                if(HttpUtils.isHttpClient(request)){
                    var readCompletionHandler = new HttpReadCompletionHandler(
                            new ConnectionMetadata("http_client".hashCode(),
                                    ConnectionMetadata.NodeType.HTTP_CLIENT,
                                    channel),
                            buffer,
                            MemoryManager.getTemporaryDirectBuffer(options.getNetworkBufferSize()),
                            httpHandler
                    );
                    try { NetworkUtils.configure(channel, options.getSoBufferSize()); } catch (IOException ignored) { }
                    readCompletionHandler.parse(new HttpReadCompletionHandler.RequestTracking());
                } else {
                    LOGGER.log(WARNING,"Leader: A node is trying to connect without a presentation message. \n" + request);
                    buffer.clear();
                    MemoryManager.releaseTemporaryDirectBuffer(buffer);
                    try { channel.close(); } catch (IOException ignored) { }
                }
                return;
            }

            // now let's do the work
            buffer.position(1);

            byte type = buffer.get();
            if(type == SERVER_TYPE){
                this.processServerPresentationMessage(channel, buffer);
            } else {
                System.out.println("Coordinator unknown type tried to connect");
                // simply unknown... probably a bug?
                LOGGER.log(WARNING,"Unknown type of client connection. Probably a bug? ");
                try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch (Exception ignored){}
            }
        }

        /**
         * Still need to define what to do with connections from replicas....
         */
        private void processServerPresentationMessage(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            // server
            ServerNode newServer = Presentation.readServer(buffer);

            // check whether this server is known... maybe it has crashed... then we only need to update the respective channel
            if(servers.get(newServer.hashCode()) != null){
                // LockConnectionMetadata connectionMetadata = serverConnectionMetadataMap.get( newServer.hashCode() );
                // update metadata of this node
                servers.put( newServer.hashCode(), newServer );
                // connectionMetadata.channel = channel;
            } else { // no need for locking here
                servers.put( newServer.hashCode(), newServer );
                LockConnectionMetadata connectionMetadata = new LockConnectionMetadata(
                        newServer.hashCode(),
                        SERVER,
                        buffer,
                        MemoryManager.getTemporaryDirectBuffer(options.getNetworkBufferSize()),
                        channel,
                        new Semaphore(1) );
                serverConnectionMetadataMap.put( newServer.hashCode(), connectionMetadata );
            }
        }
    }

    /**
     * a vms, although receiving an event from a "next" batch, cannot yet commit, since
     * there may have additional events to arrive from the current batch
     * so the batch request must contain the last tid of the given vms
     * if an internal/terminal vms do not receive an input event in this batch, it won't be able to
     * progress since the precedence info will never arrive
     * this way, we need to send in this event the precedence info for all downstream VMSs of this event
     * having this info avoids having to contact all internal/terminal nodes to inform the precedence of events
     */
    public boolean queueTransactionInput(TransactionInput transactionInput)
    {
        var dag = transactionMap.get(transactionInput.name);

        // dont accept transactions while recovering
        if (this.coordinatorIsRecovering) {
            System.out.println(STR."Denying \{dag.name}, because coordinator is recovering");
            return false;
        }

        // don't process new transactions when processing a crash
        if (this.processingCrash) {
            System.out.println(STR."Denying \{dag.name}, because crash is being processed");
            return false;
        }

        // if transaction that input is associated with is disallowed,
        // inform client that it couldn't be scheduled
        if (disallowedTransactions.contains(dag.name)) {
            System.out.println(STR."Denying \{dag.name}, because it's disallowed");
            return false;
        }

        this.transactionInputConsumer.accept(transactionInput);
        return true;
    }

    /**
     * This task assumes the channels are already established
     * Cannot have two threads writing to the same channel at the same time
     * A transaction manager is responsible for assigning TIDs to incoming transaction requests
     * This task also involves making sure the writes are performed successfully
     * A writer manager is responsible for defining strategies, policies, safety guarantees on
     * writing concurrently to channels.
     * <a href="https://web.mit.edu/6.005/www/fa14/classes/20-queues-locks/message-passing/">Message passing in Java</a>
     */
    private void processMessagesSentByVmsWorkers() {
        Object message;
        while((message = this.coordinatorQueue.poll()) != null) {
            this.processVmsMessage(message);
        }
    }

    private void processVmsMessage(Object message) {
        switch (message) {
            // receive metadata from all microservices
            case BatchContext batchContext -> this.processNewBatchContext(batchContext);
            case TxWorkerProcessedCrash txWorkerProcessedCrash -> {
                System.out.println(STR."txWorker id=\{txWorkerProcessedCrash.id} processed crash");
            }
            case VmsNode vmsNode -> this.processVmsIdentifier(vmsNode);
            case TransactionAbortAck.Payload txAbortAck ->
            {
                var abortedTid = txAbortAck.tid();
                var vmsAcknowledgedAbort = txAbortAck.vms();

                ongoingAbortMissingVmsAck.get(abortedTid).remove(vmsAcknowledgedAbort);

                var missingACKs = ongoingAbortMissingVmsAck.get(abortedTid);
                // System.out.println(STR."received ACK from \{vmsAcknowledgedAbort} for \{abortedTid}, missing fron [\{String.join(", ", missingACKs)}] in abort of \{abortedTid}");

                // all VMSes have received abort
                if (missingACKs.size() == 0)
                {
                    // incorporate with pending aborts
                    // don't resend transactions to be aborted (for optimization, not consistency)
                    resendTransactions(abortedTid);
                    ongoingAbortMissingVmsAck.remove(abortedTid);
                }
            }
            // sent only by VMSes initiating abort
            case TransactionAbortInfo.Payload txAbortInfo ->
                {
                    var abortedTid = txAbortInfo.tid();

                    // currently one abort is being processed
                    if (!ongoingAbortMissingVmsAck.isEmpty())
                    {
                        // someone tried to initiate abort of ongoing abort
                        if (ongoingAbortMissingVmsAck.containsKey(abortedTid))
                        {
                            // just remove them
                            // what if they started the abort also
                            // and they received an ACK
                            ongoingAbortMissingVmsAck.get(abortedTid).remove(txAbortInfo.vms());
                        }
                        // this is another abort
                        else
                        {
                            // store this new abort as pending and requeue
                            pendingAborts.add(txAbortInfo.tid());
                            coordinatorQueue.add(txAbortInfo);
                        }
                    }

                    // There are no ongoing abort processes already
                    else
                    {
                        // initiate abort
                        var txAbort = TransactionAbort.of(txAbortInfo.batch(), abortedTid);

                        // was this abort pending and re-queued?
                        pendingAborts.remove(abortedTid);

                        this.abortTransactionInCoordinator(txAbort, txAbortInfo.vms());

                        // all VMSes have received abort
                        var missingACKs = ongoingAbortMissingVmsAck.get(abortedTid);
                        // System.out.println(STR."coordinator SENDS abort of \{abortedTid} to [\{String.join(", ", missingACKs)}]");
                        if (missingACKs.size() == 0)
                        {
                            // incorporate with pending aborts
                            // don't resend transactions to be aborted (for optimization, not consistency)
                            resendTransactions(abortedTid);
                            ongoingAbortMissingVmsAck.remove(abortedTid);
                        }
                    }

                }
            case BatchComplete.Payload batchComplete -> this.processBatchComplete(batchComplete);
            case ReconnectionAck.Payload reconACK -> {
                var restartedVms = reconACK.vmsRestarted();
                var vmsAcknowledged = reconACK.vmsAcknowledged();
                System.out.println(STR."ACK from \{vmsAcknowledged} for reconnection to \{restartedVms} processed");

                if (!ongoingReconnectionMissingVmsAck.containsKey(restartedVms)) return;

                ongoingReconnectionMissingVmsAck.get(restartedVms).remove(vmsAcknowledged);

                var missingACKs = ongoingReconnectionMissingVmsAck.get(restartedVms);
                System.out.println(STR."Reconnection ACKs missing from [\{String.join(", ", missingACKs)}] " +
                                   STR."for reconnection of \{restartedVms}");

                // reconnection has been handled
                if (missingACKs.size() == 0) {
                    ongoingReconnectionMissingVmsAck.remove(restartedVms);
                    offlineVMSes.remove(restartedVms);

                    var dags = transactionMap.values();

                    this.disallowedTransactions = dags.stream()
                            .filter(d -> {
                                return d.getNodes().stream().anyMatch(node -> offlineVMSes.contains(node));
                            })
                            .map(d -> d.name).collect(Collectors.toSet());
                }
            }
            case CrashAck.Payload crashACK -> {
                var crashedVms = crashACK.vmsCrashed();
                var vmsAcknowledged = crashACK.vmsAcknowledged();
                System.out.println(STR."ACK from \{vmsAcknowledged} for crash of \{crashedVms} processed");

                ongoingCrashMissingVmsAck.get(crashedVms).remove(vmsAcknowledged);

                var missingACKs = ongoingCrashMissingVmsAck.get(crashedVms);
                System.out.println(STR."Crash ACKs missing from [\{String.join(", ", missingACKs)}] for crash of \{crashedVms}");

                // crash has been handled
                if (missingACKs.size() == 0) {
                    ongoingCrashMissingVmsAck.remove(crashedVms);

                    // if there are no more crashes happening, safely continue
                    if (ongoingCrashMissingVmsAck.isEmpty()) {
                        System.out.println(STR."processing crash set to false");
                        processingCrash = false;
                    }
                }
            }
            case ResetToCommittedAck.Payload payload ->
            {
                System.out.println(STR."coordinator received that \{payload.vms()} has reset to committed state");
                resetToCommittedAck.remove(payload.vms());
                if (resetToCommittedAck.isEmpty()) {
                    System.out.println(STR."coordinator is no longer recovering");
                    coordinatorIsRecovering = false;
                }
            }
            case VmsCrash.Payload vmsCrash ->
            {
                this.processCrashInCoordinator(vmsCrash);
            }
            case BatchCommitAck.Payload msg ->
                // let's just ignore the ACKs. since the terminals have responded, that means the VMSs before them have processed the transactions in the batch
                // not sure if this is correct since we have to wait for all VMSs to respond...
                // only when all vms respond with BATCH_COMMIT_ACK we move this ...
                //        this.batchOffsetPendingCommit = batchContext.batchOffset;
                    LOGGER.log(INFO, "Leader: Batch (" + msg.batch() + ") commit ACK received from " + msg.vms());
            default ->
                    LOGGER.log(WARNING, "Leader: Received an unidentified message type: " + message.getClass().getName());
        }
    }

    private void resendTransactions(long failedTid)
    {
        // don't resend transactions which will be aborted by some pending transaction abort anyway
        var minPendingAbort = pendingAborts.stream()
                .min(Long::compare)
                .orElse(Long.MAX_VALUE);

        System.out.println(STR."Coordinator resending transactions from after \{failedTid}");
        var affectedEvents =
                loggingHandler.getAffectedEvents(failedTid)
                        .stream().filter(eventRaw -> eventRaw.tid() < minPendingAbort)
                        .toList();

        for (var eventRaw: affectedEvents)
        {
            var event = TransactionEvent.read(eventRaw);

            var consumerVMSes = eventToConsumersMap.get(event.event());
            for (var consumerVms : consumerVMSes) {
                System.out.println(STR."Coordinator resending \{event.tid()} to \{consumerVms} as part of aborting \{failedTid}");
                vmsWorkerContainerMap.get(consumerVms).requeueTransactionEvent(eventRaw);
            }
        }
    }

    private void fixBatchContext(BatchContext batchContext, TransactionEvent.Payload abortedTransactionInputEvent)
    {
        // only one dag with this input event???
        var inputEventName = abortedTransactionInputEvent.event();
        var dag = transactionMap.values().stream()
                .filter(d -> d.inputEvents.containsKey(inputEventName))
                .findFirst().get();
        var dagVMSes = dag.getNodes();

        batchContext.tidAborted(abortedTransactionInputEvent.tid(), dagVMSes);
    }
    // single abort
    private void abortTransactionInCoordinator(TransactionAbort.Payload txAbort, String failedVms)
    {
        System.out.println(STR."coordinator processes abort for \{txAbort.tid()} INITIATED by \{failedVms}");

        var failedTid = txAbort.tid();
        var failedTidBatch = txAbort.batch();

        // multicast abort messages
        // 1. use VMSes already part of the ongoing abort
        var affectedVMSes = this.vmsMetadataMap.values()
                .stream()
                .map(vmsNode -> vmsNode.identifier)
                .filter(vms -> !vms.equals(failedVms)) // remove VMS that initiated from ACK requirement
                .collect(Collectors.toSet());
        for (var vms : affectedVMSes) {
            // don't need to send to the vms that aborted
            this.vmsWorkerContainerMap.get(vms).queueMessage(txAbort);
        }

        // update log
        var failedTxInputEventRaw = loggingHandler.abort(failedTid);
        var failedTxInputEvent = TransactionEvent.read(failedTxInputEventRaw);

        // registering abort
        batchAbortedTxInputs.putIfAbsent(failedTidBatch, new HashSet<>());
        batchAbortedTxInputs.get(failedTidBatch).add(failedTxInputEvent);

        // updating the batch context if it exists
        if (batchContextMap.containsKey(failedTidBatch))
        {
            var batchContext = batchContextMap.get(failedTidBatch);
            fixBatchContext(batchContext, failedTxInputEvent);
        }

        // missing ACKs
        ongoingAbortMissingVmsAck.put(failedTid, affectedVMSes);

    }

    private Set<String> computeProducerVMSes(String consumerVms)
    {
        var producerVMSes = new HashSet<String>();
        var consumerSetMap = constructVmsConsumerSet();
        for (var entry : consumerSetMap.entrySet()) {
            var vms = entry.getKey();
            var vmsConsumersStr = entry.getValue();
            if (vmsConsumersStr == null || vmsConsumersStr.trim().isEmpty()) {
                continue;
            }

            // System.out.println(STR."\{vms} has theses consumers = \{vmsConsumersStr}");
            var vmsConsumersMap= serdesProxy.deserializeConsumerSet(vmsConsumersStr);
            var vmsConsumers = vmsConsumersMap
                    .values()
                    .stream()
                    .flatMap(nodes -> nodes.stream())
                    .map(node -> node.identifier);

            var vmsIsProducer = vmsConsumers.anyMatch(consumer -> consumer.equals(consumerVms));

            // add to producer VMSes if it's a producer and is not offline
            if (vmsIsProducer) producerVMSes.add(vms);
        }
        return producerVMSes;
    }

    //////////////////////////////////////////////////////////
    //////////////////////// RECOVERY ////////////////////////
    //////////////////////////////////////////////////////////


    private void processReconnectionInCoordinator(String restartedVms)
    {

        System.out.println(STR."coordinator processing reconnection of \{restartedVms}, initiate reconnection of producers");
        var consumerSet = constructVmsConsumerSet().get(restartedVms);
        vmsWorkerContainerMap.get(restartedVms).queueMessage(consumerSet);

        var producerVMSes = computeProducerVMSes(restartedVms);
        var aliveProducers = producerVMSes.stream()
                .filter(producer -> !offlineVMSes.contains(producer))
                .collect(Collectors.toSet());

        System.out.println(STR."putting \{restartedVms} as ongoing reconnection missing ACKs " +
                           STR." from [\{String.join(", ", aliveProducers)}]");

        ongoingReconnectionMissingVmsAck.put(restartedVms, aliveProducers);

        var reconnectionMessage = VmsReconnect.of(restartedVms);
        for (var producer : aliveProducers) {
            System.out.println(STR."sending RECONNECTION to \{producer} since \{restartedVms} has reconnected");
            vmsWorkerContainerMap.get(producer).queueMessage(reconnectionMessage);
        }
    }

    private void processCrashInCoordinator(VmsCrash.Payload vmsCrash)
    {
        var crashedVms = vmsCrash.vms();
        System.out.println(STR."coordinator processing crash of \{crashedVms}");
        if (offlineVMSes.contains(crashedVms)) return;
        this.offlineVMSes.add(crashedVms);

        // delete worker
        var crashedVmsWorker = vmsWorkerContainerMap.get(crashedVms);
        if (crashedVmsWorker != null)
            crashedVmsWorker.stop();
            vmsWorkerContainerMap.remove(crashedVms);

        processingCrash = true;
        coordinatorQueue.clear();
        clearTransactionInputs();

        // find the DAGs that use the crashed VMS
        var dags = transactionMap.values();
        var affectedDags = dags.stream()
                .filter(d -> d.getNodes().contains(crashedVms))
                .toList();
        var affectedTransactions = affectedDags.stream().map(d -> d.name).toList();
        disallowedTransactions.addAll(affectedTransactions);

        System.out.println(STR."coordinator disallows vms: \{crashedVms}");
        for (var tx : affectedTransactions) {
            System.out.println(STR."coordinator disallows: \{tx}");
        }

        destroyTransactionWorkers();
        long tid;
        long bid;
        try {
            var commitInfo = loggingHandler.latestCommit();
            tid = commitInfo[1];
            bid = commitInfo[0];
        } catch (Exception e) {
            tid = 0;
            bid = 0;
        }

        // the VMSes online at the time of crash
        // there is only a worker if the VMS is alive.
        var crashParticipants = vmsMetadataMap.values()
                .stream()
                .map(node -> node.identifier)
                .filter(vms -> !offlineVMSes.contains(vms))
                .collect(Collectors.toSet());
        ongoingCrashMissingVmsAck.put(crashedVms, crashParticipants);
        for (var vms : crashParticipants) {
            vmsWorkerContainerMap.get(vms).queueMessage(vmsCrash);
        }

        setUpTransactionWorkers(tid+1, bid+1);

        // UPDATE METADATA
        final long finalBid = bid;
        batchContextMap.keySet().removeIf(batch -> batch > finalBid);
        batchOffsetPendingCommit = bid + 1;
    }

    private void processBatchComplete(BatchComplete.Payload batchComplete) {
        // what if ACKs from VMSs take too long? or never arrive?
        // need to deal with intersecting batches? actually just continue emitting for higher throughput
        LOGGER.log(DEBUG,"Leader: Processing batch ("+ batchComplete.batch()+") complete from: "+ batchComplete.vms());
        BatchContext batchContext = this.batchContextMap.get( batchComplete.batch() );
        // only if it is not a duplicate vote
        batchContext.missingVotes.remove( batchComplete.vms() );
        // System.out.println(STR."Coordinator batch complete from \{batchComplete.vms()} and missingVotes=\{batchContext.missingVotes.size()}");
        if(batchContext.missingVotes.isEmpty()){
            LOGGER.log(INFO,"Leader: Received all missing votes of batch "+ batchContext.batchOffset + " with "+batchContext.numTIDsOverall+" transactions");
            this.updateBatchOffsetPendingCommit(batchContext);
        }
    }

    private final AtomicLong numTIDsCommitted = new AtomicLong(0);

    private void updateBatchOffsetPendingCommit(BatchContext batchContext)
    {
        if(batchContext.batchOffset == this.batchOffsetPendingCommit) {
            // STORE committed batch events persistently
            loggingHandler.commit(batchOffsetPendingCommit);

            this.numTIDsCommitted.updateAndGet(i -> i + batchContext.numTIDsOverall);
            this.sendCommitCommandToVMSs(batchContext);


            this.batchOffsetPendingCommit = batchContext.batchOffset + 1;
            // making this implementation order-independent, so not assuming batch commit are received in order
            BatchContext nextBatchContext = this.batchContextMap.get( this.batchOffsetPendingCommit );
            if(nextBatchContext != null && nextBatchContext.missingVotes.isEmpty()){
                this.updateBatchOffsetPendingCommit(nextBatchContext);
            }

            return;
        }
        // probably some batch complete message got lost or received out of order
        LOGGER.log(WARNING,"Leader: Batch ("+ batchContext.batchOffset +
                ") is not the pending one.\nStill has to wait for the pending batch ("+
                this.batchOffsetPendingCommit+") to finish before progressing. Nodes that still need to vote:\n"+
                (this.batchContextMap.containsKey( this.batchOffsetPendingCommit ) ?
                        this.batchContextMap.get( this.batchOffsetPendingCommit )
                                .missingVotes : Set.of()));
    }

    // seal batch and send batch complete to all terminals...
    private void processNewBatchContext(BatchContext batchContext)
    {
        var batch = batchContext.batchOffset;

        //
        var abortedTxInputs = batchAbortedTxInputs.get(batch);
        if (abortedTxInputs != null) {
            for (var input : abortedTxInputs)
            {
                fixBatchContext(batchContext, input);
            }
        }

        this.batchContextMap.put(batch, batchContext);

        // after storing batch context, send to vms workers
        for(var entry : batchContext.terminalVMSs) {
            this.vmsWorkerContainerMap.get(entry).queueMessage(
                    BatchCommitInfo.of(batchContext.batchOffset,
                            batchContext.previousBatchPerVms.get(entry),
                            batchContext.numberOfTIDsPerVms.get(entry),
                            batchContext.abortedTIDs));
        }
    }

    private void processVmsIdentifier(VmsNode vmsIdentifier_) {
        LOGGER.log(INFO,"Leader: Received a VMS_IDENTIFIER from: "+ vmsIdentifier_.identifier);

        // second part of reconnection
        // sending out the
        var vmsIdentifier = vmsIdentifier_.identifier;
        if (vmsMetadataMap.containsKey(vmsIdentifier) && offlineVMSes.contains(vmsIdentifier))
        {
            System.out.println(STR."\{vmsIdentifier} introduced itself again to the coordinator");
            processReconnectionInCoordinator(vmsIdentifier);
            return;
        }

        // update metadata of this node so coordinator can reason about data dependencies
        this.vmsMetadataMap.put( vmsIdentifier_.identifier, vmsIdentifier_);

        if(this.vmsMetadataMap.size() < this.starterVMSs.size()) {
            LOGGER.log(INFO,"Leader: "+(this.starterVMSs.size() - this.vmsMetadataMap.size())+" starter(s) VMSs remain to be processed.");
            return;
        }

        // if all metadata, from all starter vms have arrived, then send the signal to them

        LOGGER.log(INFO,"Leader: All VMS starters have sent their VMS_IDENTIFIER");

        // new VMS may join, requiring updating the consumer set
        var consumerSetMap = constructVmsConsumerSet();
        for (var vms : consumerSetMap.keySet()) {

            if (!coordinatorIsRecovering)
            {
                this.vmsWorkerContainerMap.get(vms).queueMessage(consumerSetMap.get(vms));
            }
        }
    }

    private Map<String, String> constructVmsConsumerSet()
    {
        Map<String, String> consumerSetMap = new HashMap<>();
        Map<String, List<IdentifiableNode>> vmsConsumerSet;
        for(VmsNode vmsNode : this.vmsMetadataMap.values()) {
            vmsConsumerSet = new HashMap<>();

            IdentifiableNode vms = this.starterVMSs.get(vmsNode.identifier);
            if (vms == null) {
                continue;
            }

            // build global view of vms dependencies/interactions
            // build consumer set dynamically
            // for each output event, find the consumer VMSs
            for (VmsEventSchema eventSchema : vmsNode.outputEventSchema.values()) {
                List<IdentifiableNode> nodes = this.findConsumerVMSs(eventSchema.eventName);
                if (!nodes.isEmpty())
                    vmsConsumerSet.put(eventSchema.eventName, nodes);
            }

            String consumerSetStr = "";
            if (!vmsConsumerSet.isEmpty()) {
                consumerSetStr = this.serdesProxy.serializeConsumerSet(vmsConsumerSet);
            }
            consumerSetMap.put(vmsNode.identifier, consumerSetStr);
        }
        return consumerSetMap;
    }

    private void sendCommitCommandToVMSs(BatchContext batchContext){
        for(VmsNode vms : this.vmsMetadataMap.values()){

            // has this VMS participated in this batch?
            if(!batchContext.numberOfTIDsPerVms.containsKey(vms.identifier)) {
                LOGGER.log(DEBUG,"Leader: Batch ("+batchContext.batchOffset+") commit command will not be sent to "+ vms.identifier + " because this VMS has not participated in this batch.");
                continue;
            }
//            System.out.println(STR."Sending commit command to \{vms.identifier} " +
//                               STR."with \{batchContext.numberOfTIDsPerVms.get(vms.identifier)} TIDs processed");

            this.vmsWorkerContainerMap.get(vms.identifier).queueMessage(
                    new BatchCommitCommand.Payload(
                        batchContext.batchOffset,
                        batchContext.previousBatchPerVms.get(vms.identifier),
                        batchContext.numberOfTIDsPerVms.get(vms.identifier),
                        batchContext.abortedTIDs
            ));
        }

        if(!BATCH_COMMIT_CONSUMERS.isEmpty()) {
            final long tid = this.numTIDsCommitted.get();
            BATCH_COMMIT_CONSUMERS.forEach(c->c.accept(tid));
            /* must test both approaches in the experiments
            for (var task : BATCH_COMMIT_CONSUMERS){
                submitBackgroundTask(()-> task.accept(tid));
            }
             */
        }
    }

    public long getNumTIDsCommitted() {
        return this.numTIDsCommitted.get();
    }

    public long getNumTIDsSubmitted(){
        long sumTIDs = 0;
        for(var txWorker : this.transactionWorkers){
            sumTIDs += txWorker.t1().getNumTIDsSubmitted();
        }
        return sumTIDs;
    }

    public CoordinatorOptions getOptions(){
        return this.options;
    }

    public Map<String, VmsNode> getConnectedVMSs() {
        return this.vmsMetadataMap;
    }

    public long getBatchOffsetPendingCommit() {
        return this.batchOffsetPendingCommit;
    }

    public Map<String, IdentifiableNode> getStarterVMSs(){
        return this.starterVMSs;
    }

}
