package dk.ku.di.dms.vms.tpcc.benchmark.ingestion;

import dk.ku.di.dms.vms.tpcc.benchmark.MinimalHttpClient;

import java.util.function.BiFunction;

public abstract class IngestionWorker implements Runnable
{
    public IngestionWorker()
    {
    }
    protected static BiFunction<String, Integer, MinimalHttpClient> HTTP_CLIENT_SUPPLIER = (host, port) ->
    {
        try {
            return new MinimalHttpClient(host, port);
        } catch (Exception e) {
            throw new RuntimeException("");
        }
    };
}
