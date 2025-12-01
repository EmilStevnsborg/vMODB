package dk.ku.di.dms.vms.flightScheduler.benchmark.experiment;

public record CrashInfo(String crashedVms, long timestampProcessed, long timestampAcknowledged) {}
