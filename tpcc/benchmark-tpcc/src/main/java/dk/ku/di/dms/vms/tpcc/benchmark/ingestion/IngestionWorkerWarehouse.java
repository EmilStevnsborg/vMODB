package dk.ku.di.dms.vms.tpcc.benchmark.ingestion;

import dk.ku.di.dms.vms.tpcc.benchmark.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenerator;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Customer;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import static dk.ku.di.dms.vms.tpcc.benchmark.Constants.WAREHOUSE_PORT;

public class IngestionWorkerWarehouse extends IngestionWorker
{
    public IngestionWorkerWarehouse() {
        super();
    }

    @Override
    public void run()
    {
        // just one warehouse: # 1
        try
        {
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply("localhost", WAREHOUSE_PORT);
            Warehouse warehouse = DataGenerator.generateWarehouse(1);
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
