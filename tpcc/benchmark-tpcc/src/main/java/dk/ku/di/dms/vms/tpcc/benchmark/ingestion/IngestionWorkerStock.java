package dk.ku.di.dms.vms.tpcc.benchmark.ingestion;

import dk.ku.di.dms.vms.tpcc.benchmark.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenerator;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Stock;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import java.util.Date;
import java.util.HashMap;

import static dk.ku.di.dms.vms.tpcc.benchmark.Constants.INVENTORY_PORT;

public class IngestionWorkerStock extends IngestionWorker
{
    public IngestionWorkerStock() {
        super();
    }

    @Override
    public void run()
    {
        // just one warehouse: # 1
        try
        {
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply("localhost", INVENTORY_PORT);
            for (int i = 0; i < TPCcConstants.NUM_ITEMS; i++) {
                Stock stock = DataGenerator.generateStockItem(1, i);
                client.sendRequest("POST", stock.toString(), "stock");
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
