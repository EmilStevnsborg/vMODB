package dk.ku.di.dms.vms.tpcc.benchmark.experiment;

public record ReconnectionInfo(String restartedVms, long timestampProcessed, long timestampAcknowledged) {}