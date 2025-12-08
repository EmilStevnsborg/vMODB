package dk.ku.di.dms.vms.tpcc.benchmark.experiment;

public record ThroughputInfo(long timestampStart, long timestampEnd, long numCommittedTiDs) {}
