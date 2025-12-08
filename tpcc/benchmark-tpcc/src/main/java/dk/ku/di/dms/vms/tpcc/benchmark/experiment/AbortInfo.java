package dk.ku.di.dms.vms.tpcc.benchmark.experiment;

public record AbortInfo(long abortedTid, long timestampProcessed, long timestampAcknowledged) {}
