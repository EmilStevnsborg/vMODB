package dk.ku.di.dms.vms.tpcc.benchmark.experiment;

public record BatchStats(long batchId, long numCommitedTiDs, long endTs){}
