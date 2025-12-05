package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsTransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.ISchedulerCallback;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskBuilder;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskBuilder.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.scheduler.complex.VmsComplexTransactionScheduler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static java.lang.System.Logger.Level.*;

/**
 * A transaction scheduler aware of partitioned and parallel tasks.
 * Besides, for simplicity, it only considers transactions (i.e., event inputs) that spawn a single task
 * in each VMS (in opposite of possible many tasks as found in {@link VmsComplexTransactionScheduler}).
 */
public final class VmsTransactionScheduler extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(VmsTransactionScheduler.class.getName());

    // must be concurrent since different threads are writing and reading from it concurrently
    private final ConcurrentHashMap<Long, VmsTransactionTask> transactionTaskMap;
    private Map<Long, Set<Long>> batchAbortedTIDs;
    private AtomicLong currentGeneration;

    // map the last tid
    private final Map<Long, Long> lastTidToTidMap;

    /**
     * Thread pool for partitioned and parallel tasks
     */
    private final ExecutorService sharedTaskPool;

    private final AtomicInteger numParallelTasksRunning = new AtomicInteger(0);

    private final AtomicInteger numPartitionedTasksRunning = new AtomicInteger(0);

    private volatile boolean singleThreadTaskRunning = false;

    // the callback atomically updates this variable
    // used to track progress in the presence of parallel and partitioned tasks
    private final AtomicLong lastTidFinished;

    private final Set<Object> partitionKeyTrackingMap = ConcurrentHashMap.newKeySet();

    private final BlockingQueue<InboundEvent> transactionInputQueue;

    // transaction metadata mapping
    private final Map<String, VmsTransactionMetadata> transactionMetadataMap;

    // used to identify in which VMS this scheduler is running
    private final String vmsIdentifier;

    private final VmsTransactionTaskBuilder vmsTransactionTaskBuilder;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public static VmsTransactionScheduler build(String vmsIdentifier,
                                                BlockingQueue<InboundEvent> transactionInputQueue,
                                                Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                                ITransactionManager transactionalHandler,
                                                Consumer<IVmsTransactionResult> eventHandler,
                                                Map<Long, Set<Long>> vmsBatchAbortedTIDs, AtomicLong generation,
                                                int vmsThreadPoolSize){
        LOGGER.log(INFO, vmsIdentifier+ ": Building transaction scheduler with thread pool size of "+ vmsThreadPoolSize);
        return new VmsTransactionScheduler(
                vmsIdentifier,
                vmsThreadPoolSize == 0 ? ForkJoinPool.commonPool() :
                        Executors.newFixedThreadPool( vmsThreadPoolSize, Thread.ofPlatform().name("vms-task-thread").factory() ),
                transactionInputQueue,
                transactionMetadataMap,
                transactionalHandler,
                eventHandler,
                vmsBatchAbortedTIDs, generation);
    }

    private VmsTransactionScheduler(String vmsIdentifier,
                                    ExecutorService sharedTaskPool,
                                    BlockingQueue<InboundEvent> transactionInputQueue,
                                    Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                    ITransactionManager transactionalHandler,
                                    Consumer<IVmsTransactionResult> eventHandler,
                                    Map<Long, Set<Long>> vmsBatchAbortedTIDs, AtomicLong generation){
        super();

        this.vmsIdentifier = vmsIdentifier;
        // thread pools
        this.sharedTaskPool = sharedTaskPool;

        // infra (come from external)
        this.transactionMetadataMap = transactionMetadataMap;
        this.transactionInputQueue = transactionInputQueue;

        // operational (internal control of transactions and tasks)
        this.transactionTaskMap = new ConcurrentHashMap<>(1000000);
        SchedulerCallback callback = new SchedulerCallback(eventHandler);
        this.batchAbortedTIDs = vmsBatchAbortedTIDs;
        this.currentGeneration = generation;
        this.vmsTransactionTaskBuilder = new VmsTransactionTaskBuilder(transactionalHandler, callback);
        this.transactionTaskMap.put( 0L, this.vmsTransactionTaskBuilder.buildFinished(0, this.currentGeneration.get() ) );
        this.lastTidToTidMap = new HashMap<>(1000000);

        this.lastTidFinished = new AtomicLong(0);
    }

    /**
     * Inspired by <a href="https://stackoverflow.com/questions/826212/java-executors-how-to-be-notified-without-blocking-when-a-task-completes">link</a>,
     * this method can block on checkForNewEvents, leaving the task threads itself, via callback, modify
     * the class state appropriately. Care must be taken with some variables.
     */
    @Override
    public void run() {
        LOGGER.log(INFO,this.vmsIdentifier+": Transaction scheduler has started");
        while(this.isRunning()) {

            // check if tasks need to be cleared

            // stops scheduling new tasks
            if (this.isPaused()) continue;

            try {
                this.checkForNewEvents();
                this.executeReadyTasks();
            } catch(Exception e){
                e.printStackTrace(System.out);
                LOGGER.log(ERROR, this.vmsIdentifier+": Error on scheduler loop: "+(e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
            }
        }
        LOGGER.log(INFO,this.vmsIdentifier+": Transaction scheduler has terminated");
    }

    private final class SchedulerCallback implements ISchedulerCallback, Thread.UncaughtExceptionHandler {

        private final Consumer<IVmsTransactionResult> eventHandler;

        private SchedulerCallback(Consumer<IVmsTransactionResult> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        public void success(ExecutionModeEnum executionMode, OutboundEventResult outboundEventResult) {
            VmsTransactionTask task = transactionTaskMap.get(outboundEventResult.tid());
            task.signalFinished();
            updateLastFinishedTid(outboundEventResult.tid());
            this.eventHandler.accept(outboundEventResult);
            this.updateSchedulerTaskStats(executionMode, task);
        }

        @Override
        public void error(ExecutionModeEnum executionMode, long tid, long batch, long generation, Exception e) {
            // a simple mechanism to handle error is by re-executing, depending on the nature of the error
            // if constraint violation, it cannot be re-executed
            // in this case, the error must be informed to the event handler, so the event handler
            // can forward the error to downstream VMSs. if input VMS, easier to handle, just send a noop to them

            // remove from map to avoid reescheduling? no, it will lead to null pointer in scheduler loop
            VmsTransactionTask task = transactionTaskMap.get(tid);
            task.signalFailed();
            this.updateSchedulerTaskStats(executionMode, task);

            if (isPaused()) {
                // dont signal
            }

            // abort task now
            var eventOutput = new OutboundEventResult(tid, batch, generation); // abort
            this.eventHandler.accept(eventOutput);
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            LOGGER.log(ERROR, "Uncaught exception captured during application execution: \n"+e.getCause().getMessage());
        }

        private void updateSchedulerTaskStats(ExecutionModeEnum executionMode, VmsTransactionTask task) {
            switch (executionMode){
                case SINGLE_THREADED -> singleThreadTaskRunning = false;
                case PARALLEL -> numParallelTasksRunning.decrementAndGet();
                case PARTITIONED -> {
                    if(!task.partitionKeys().isEmpty()){
                        for(var partitionKey : task.partitionKeys()) {
                            if (!partitionKeyTrackingMap.remove(partitionKey)) {
                                LOGGER.log(WARNING, vmsIdentifier + ": Partitioned task " + task.tid() + " did not find its partition ID (" + partitionKey + ") in the tracking map!");
                            }
                        }
                        numPartitionedTasksRunning.decrementAndGet();
                        LOGGER.log(DEBUG, vmsIdentifier + ": Partitioned task " + task.tid() + " finished execution.");
                    } else {
                        singleThreadTaskRunning = false;
                    }
                }
            }
        }
    }

    /**
     * This method makes sure that TIDs always increase
     * so the next single thread tasks can be executed
     */
    private void updateLastFinishedTid(final long tid){
        // System.out.println(STR."\{vmsIdentifier} update lasFinishedTid to \{tid} instead of \{this.lastTidFinished.get()}");
        if(this.lastTidFinished.get() > tid) return;
        this.lastTidFinished.updateAndGet(currTid -> Math.max(currTid, tid));
    }


    /**
     * To avoid the scheduler to remain in a busy loop while no new input events arrive
     */
    private boolean mustWaitForInputEvent = false;

    @Override
    public void pauseHandler(boolean pause)
    {
        // System.out.println(STR."\{vmsIdentifier}-SCHEDULER pausing set to \{pause}");
        var RUNNING = 2;
        if (pause) {
            pause();

            // wait for tasks to finish
            for (var task : transactionTaskMap.values()) {
                // wait for running tasks to finish
                while (task.status() == RUNNING) {}
            }

            // also stop processing tasks
            while(!drained.isEmpty()) {}
        } else {
            resume();
        }
    }

    @Override
    public void recover(long lastCommitBatch,  long lastCommitTid)
    {
//        System.out.println(STR."Scheduler for \{vmsIdentifier} setting lastTidFinished=lastCommitTid=\{lastCommitTid}, " +
//                           STR."where lastCommitBatch=\{lastCommitBatch}");
        lastTidFinished.set(lastCommitTid);
    }

    @Override
    public Long[] taskClearer(Tuple<Long, Long> tidBidInfo, boolean isLatestCommitted)
    {
        // need to acquire write locks on lastTidToTidMap

        var tid = tidBidInfo.t1();
        var bid = tidBidInfo.t2();
        // System.out.println(STR."\{vmsIdentifier} clearing tasks at and later than \{failedTid}");

        // input queue may contain
        // block the queue
        // filter it

        long numTIDsExecuted = 0;
        var lastTidToTidMapIterator = lastTidToTidMap.entrySet().iterator();
        while (lastTidToTidMapIterator.hasNext()) {
            var entry = lastTidToTidMapIterator.next();
            if (entry == null) break;

            // transactions after failedTid
            if (entry.getKey() >= tid) {
//                System.out.println(STR."REMOVES lastTidToTid of \{entry.getKey()} in \{vmsIdentifier}");
                lastTidToTidMapIterator.remove();
            }
            if (entry.getValue() == tid) {
//                System.out.println(STR."REMOVES lastTidToTid of \{entry.getKey()} to \{tid} in \{vmsIdentifier}");
                lastTidToTidMapIterator.remove();
            }
        }

        // the task of the supposed failed transaction. May be null if aborted during recovery
        var transactionTaskMapIterator = transactionTaskMap.entrySet().iterator();
        while (transactionTaskMapIterator.hasNext()) {
            var entry = transactionTaskMapIterator.next();
            if (entry == null) break;

            var taskTid = entry.getKey();
            VmsTransactionTask task = entry.getValue();
            var taskBatchId = task.batch;
            if (taskTid >= tid) {
                // System.out.println(STR."REMOVES task \{taskTid} with lastTid=\{task.lastTid} as part of abort for \{failedTid} in \{vmsIdentifier}");
                transactionTaskMapIterator.remove();
            }

            // count the events
            if (taskBatchId == bid && taskTid < tid) {
                numTIDsExecuted += 1;
            }
        }

        if (isLatestCommitted) {
            // System.out.println(STR."\{vmsIdentifier}-SCHEDULER task clearer is part of reset");
            // tasks are never removed
            lastTidFinished.set(tid - 1);
        } else {
            var floorEntry = transactionTaskMap.entrySet()
                    .stream()
                    .filter(e -> e.getValue().isFinished() && e.getKey() < tid)
                    .map(e -> e.getKey())
                    .max(Long::compare)
                    .orElse(0L);

            // tasks are never removed
            lastTidFinished.set(floorEntry);
        }

        var maxTidExecuted = lastTidFinished.get();

        System.out.println(STR."\{vmsIdentifier}-SCHEDULER: sets lastTidFinished to \{lastTidFinished} when clearing for \{tid}");
        return new Long[] {numTIDsExecuted, maxTidExecuted};
    }

    private void executeReadyTasks()
    {
        Long nextTid = this.lastTidToTidMap.get(this.lastTidFinished.get());
        // System.out.println(STR."nextTid=\{nextTid} in \{vmsIdentifier}");

        // if nextTid == null then the scheduler must block until a new event arrive to progress
        if(nextTid == null) {
            // keep scheduler sleeping since next tid is unknown
            if (!mustWaitForInputEvent) {
//                System.out.println(STR."\{vmsIdentifier}-SCHEDULER mustWaitForInputEvent=true");
                this.mustWaitForInputEvent = true;
            }
            return;
        }
        VmsTransactionTask task = this.transactionTaskMap.get( nextTid );
        if (task == null) {
            // System.out.println(STR."task is null for \{nextTid}");
            return;
        }
        // System.out.println(STR."\{vmsIdentifier}-SCHEDULER nextTID for lastTid=\{this.lastTidFinished.get()} is \{nextTid}");
        while(true) {
            if(task.isScheduled()){
                // System.out.println(STR."task \{task} with lastTidFinished=\{lastTidFinished} in \{vmsIdentifier} isScheduled");
                return;
            }
            // must check because partitioned task interleave and may finish before a lower TID
            if(task.isFinished()){
                // System.out.println(STR."task \{task} with lastTidFinished=\{lastTidFinished} in \{vmsIdentifier} isFinished");
                if (this.isPaused()) {
                    System.out.println(STR."Scheduler is paused");
                    return;
                }
                this.updateLastFinishedTid(nextTid);
                return;
            }
            if (task.isFailed()) {
                // System.out.println(STR."task \{task} with lastTidFinished=\{lastTidFinished} in \{vmsIdentifier} isFailed");
                return;
            }
            // System.out.println(STR."\{vmsIdentifier} Execute task tid=\{task.tid()}");
            switch (task.signature().executionMode()) {
                case SINGLE_THREADED -> {
                    if (!this.canSingleThreadTaskRun()) {
                        return;
                    }
                    this.submitSingleThreadTaskForExecution(task);
                }
                case PARALLEL -> {
                    if (!this.canParallelTaskRun()) {
                        return;
                    }
                    this.numParallelTasksRunning.incrementAndGet();
                    task.signalReady();
                    this.sharedTaskPool.submit(task);
                }
                case PARTITIONED -> {
                    if(task.partitionKeys().isEmpty()){
                        if(this.canSingleThreadTaskRun()){
                            this.submitSingleThreadTaskForExecution(task);
                        }
                        return;
                    }
                    if (!this.canPartitionedTaskRun()) { return; }
                    for(Object partitionKey : task.partitionKeys()){
                        if(this.partitionKeyTrackingMap.contains(partitionKey)) return;
                    }
                    this.submitPartitionedTaskForExecution(task);
                }
            }
            // bypass the single-thread execution if possible
            if(!this.singleThreadTaskRunning && this.lastTidToTidMap.containsKey( task.tid() )){
                task = this.transactionTaskMap.get( this.lastTidToTidMap.get( task.tid() ) );
            }
        }
    }

    private void submitPartitionedTaskForExecution(VmsTransactionTask task) {
        this.partitionKeyTrackingMap.addAll(task.partitionKeys());
        this.numPartitionedTasksRunning.incrementAndGet();
        task.signalReady();
        this.sharedTaskPool.submit(task);
    }

    private void submitSingleThreadTaskForExecution(VmsTransactionTask task) {
        // System.out.println(STR."submitSingleThreadTaskForExecution task.tid=\{task.tid()} isPaused=\{isPaused()}");
        this.singleThreadTaskRunning = true;
        task.signalReady();
        // can the scheduler itself run it? if so, avoid a context switch cost
        // but blocks the scheduler until the task finishes
        this.sharedTaskPool.submit(task);
    }

    private boolean canSingleThreadTaskRun() {
        var runnable = !this.singleThreadTaskRunning && this.numParallelTasksRunning.get() == 0 && numPartitionedTasksRunning.get() == 0;
        // System.out.println(STR."\{vmsIdentifier}-SCHEDULER canSingleThreadTaskRun=\{runnable}");
        return runnable;
    }

    private boolean canPartitionedTaskRun(){
        var runnable = !this.singleThreadTaskRunning && this.numParallelTasksRunning.get() == 0;
//        System.out.println(STR."\{vmsIdentifier}-SCHEDULER canPartitionedTaskRun=\{runnable}");
        return runnable;
    }

    private boolean canParallelTaskRun(){
        var runnable = !this.singleThreadTaskRunning && this.numPartitionedTasksRunning.get() == 0;
//        System.out.println(STR."\{vmsIdentifier}-SCHEDULER canParallelTaskRun=\{runnable}");
        return runnable;
    }

    private final List<InboundEvent> drained = new ArrayList<>(1024*10);

    private void checkForNewEvents() throws InterruptedException {
        InboundEvent inboundEvent;
        if(this.mustWaitForInputEvent) {
            // blocking (causes execution problems in tests)
            inboundEvent = this.transactionInputQueue.take();
            // disable block
            this.mustWaitForInputEvent = false;
            // System.out.println(STR."\{vmsIdentifier}-SCHEDULER mustWaitForInputEvent=false");
        } else {
            inboundEvent = this.transactionInputQueue.poll();
            if(inboundEvent == null) return;
        }
        // drain all
        this.drained.add(inboundEvent);
        this.transactionInputQueue.drainTo(this.drained);
        for(InboundEvent inboundEvent_ : this.drained){
            this.processNewEvent(inboundEvent_);
        }
        this.drained.clear();
    }

    private void processNewEvent(InboundEvent inboundEvent)
    {
        if (inboundEvent.generation() != this.currentGeneration.get()) {
            System.out.println(STR."\{vmsIdentifier} inboundEvent gen=\{inboundEvent.generation()} != " +
                               STR."currentGeneration=\{currentGeneration} " +
                               STR."of inbound event \{inboundEvent.tid()} ignored");
            return;
        }

        if (this.transactionTaskMap.containsKey(inboundEvent.tid())) {
//            var task = transactionTaskMap.get(inboundEvent.tid());
//            System.out.println(STR."\{vmsIdentifier} inbound \{inboundEvent.tid()} with lastTid=\{inboundEvent.lastTid()} " +
//                               STR."is duplicate for task with lastTid=\{task.lastTid}");

            return;
        }
        var abortedTIDs = batchAbortedTIDs.get(inboundEvent.batch());

        // deprecated event
        //   1. if it is aborted
        //   2. if the lastTid is aborted
        var deprecated = abortedTIDs != null &&
                (abortedTIDs.contains(inboundEvent.tid()) ||
                        abortedTIDs.contains(inboundEvent.lastTid()));

//        System.out.println(STR."\{vmsIdentifier} tid=\{inboundEvent.tid()} deprecated=\{deprecated}, " +
//                STR."lastTid=\{inboundEvent.lastTid()}, lastTidFinished=\{lastTidFinished}," +
//                STR."inbound.generation=\{inboundEvent.generation()}, currentGeneration=\{currentGeneration}");

        // deprecated event
        if (deprecated) {
            return;
        }

        // if the event is marked as aborted, create a pseudo task,
        // that simply takes the inbound event and creates a
        // pseudo output event

        this.transactionTaskMap.put(inboundEvent.tid(), this.vmsTransactionTaskBuilder.build(
                inboundEvent.tid(),
                inboundEvent.lastTid(),
                inboundEvent.batch(),
                inboundEvent.generation(),
                this.transactionMetadataMap
                        .get(inboundEvent.event())
                        .signatures.getFirst().object(),
                inboundEvent.input()
        ));
         // System.out.println(STR."\{vmsIdentifier}-SCHEDULER: put new task for tid=\{inboundEvent.tid()} with lastTid=\{inboundEvent.lastTid()}, current lastTidFinished=\{lastTidFinished}");

        // mark the last tid, so we can get the next to execute when appropriate
        if(this.lastTidToTidMap.containsKey(inboundEvent.lastTid())){
            var containedTiD = this.lastTidToTidMap.get(inboundEvent.lastTid());
            if (abortedTIDs.contains(containedTiD)) {
                this.lastTidToTidMap.put(inboundEvent.lastTid(), inboundEvent.tid());
//                System.out.println(STR."\{vmsIdentifier}-SCHEDULER: put \{inboundEvent.tid()} in map for \{inboundEvent.lastTid()}");
                return;
            }
//            System.out.println(STR."\{vmsIdentifier}-SCHEDULER: containsKey for lastTid()=\{inboundEvent.lastTid()} for " +
//                               STR."\{lastTidToTidMap.get(inboundEvent.lastTid())} when received inboundEvent tid=\{inboundEvent.tid()}");
        } else {
            this.lastTidToTidMap.put(inboundEvent.lastTid(), inboundEvent.tid());
//            System.out.println(STR."\{vmsIdentifier}-SCHEDULER: put \{inboundEvent.tid()} in map for \{inboundEvent.lastTid()}");
        }
    }

    public long lastTidFinished(){
        return this.lastTidFinished.get();
    }

}
