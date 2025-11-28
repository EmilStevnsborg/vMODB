package dk.ku.di.dms.vms.tpcc.proxy.experiment;

public record ThroughputInfo(long timestampStart, long timestampEnd, long numCommittedTiDs) {}
