package dk.ku.di.dms.vms.flightScheduler.benchmark.experiment;

public record AbortInfo(long abortedTid, long timestampProcessed, long timestampAcknowledged) {}
