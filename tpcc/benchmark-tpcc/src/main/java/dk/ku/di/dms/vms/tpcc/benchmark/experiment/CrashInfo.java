package dk.ku.di.dms.vms.tpcc.benchmark.experiment;

public record CrashInfo(String crashedVms, long timestampProcessed, long timestampAcknowledged) {}
