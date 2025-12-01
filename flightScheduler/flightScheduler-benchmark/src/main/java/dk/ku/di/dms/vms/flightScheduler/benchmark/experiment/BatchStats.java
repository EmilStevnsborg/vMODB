package dk.ku.di.dms.vms.flightScheduler.benchmark.experiment;

public record BatchStats(long batchId, long numCommitedTiDs, long endTs){}
