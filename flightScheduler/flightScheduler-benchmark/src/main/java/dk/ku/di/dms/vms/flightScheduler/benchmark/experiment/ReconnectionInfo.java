package dk.ku.di.dms.vms.flightScheduler.benchmark.experiment;

public record ReconnectionInfo(String restartedVms, long timestampProcessed, long timestampAcknowledged) {}