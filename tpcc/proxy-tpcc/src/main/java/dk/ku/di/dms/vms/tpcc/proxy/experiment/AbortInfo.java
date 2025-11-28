package dk.ku.di.dms.vms.tpcc.proxy.experiment;

public record AbortInfo(long abortedTid, long timestampProcessed, long timestampAcknowledged) {}
