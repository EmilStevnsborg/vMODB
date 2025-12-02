package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.sdk.core.scheduler.IVmsTransactionResult;

/**
 * Just a placeholder.
 * The object needs to be converted before being sent
 */
public final class OutboundEventResult implements IVmsTransactionResult {

    private final long tid;
    private final long batch;
    private final long generation;
    private final boolean isAbort;
    private final String outputQueue;
    private final Object output;

    public OutboundEventResult(long tid, long batch, long generation, String outputQueue, Object output) {
        this.tid = tid;
        this.batch = batch;
        this.generation = generation;
        this.outputQueue = outputQueue;
        this.output = output;
        this.isAbort = false;
    }
    // I made this and isAbort()
    public OutboundEventResult(long tid, long batch, long generation) {
        this.tid = tid;
        this.batch = batch;
        this.generation = generation;
        this.isAbort = true;
        this.outputQueue = null;
        this.output = null;
    }

    @Override
    public long tid() {
        return this.tid;
    }

    @Override
    public OutboundEventResult getOutboundEventResult() {
        return this;
    }

    public boolean isAbort() {
        return this.isAbort;
    }
    public String outputQueue() {
        return this.outputQueue;
    }

    public long batch() {
        return this.batch;
    }
    public long generation() {
        return this.generation;
    }

    public Object output() {
        return this.output;
    }
}