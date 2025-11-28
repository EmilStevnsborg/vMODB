package dk.ku.di.dms.vms.flightScheduler.benchmark.experiment;

public record ThroughputInfo(long timestampStart, long timestampEnd, long numCommittedTiDs) {}
