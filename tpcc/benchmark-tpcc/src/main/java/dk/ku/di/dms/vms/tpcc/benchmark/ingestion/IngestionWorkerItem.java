package dk.ku.di.dms.vms.tpcc.benchmark.ingestion;

import dk.ku.di.dms.vms.tpcc.benchmark.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenerator;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Item;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import static dk.ku.di.dms.vms.tpcc.benchmark.Constants.INVENTORY_PORT;
import static dk.ku.di.dms.vms.tpcc.benchmark.Constants.WAREHOUSE_PORT;

public class IngestionWorkerItem extends IngestionWorker
{
    public IngestionWorkerItem() {
        super();
    }

    @Override
    public void run()
    {
        // just one warehouse: # 1
        try
        {
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply("localhost", INVENTORY_PORT);
            for(int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++) {
                Item item = DataGenerator.generateItem(i_id);
                client.sendRequest("POST", item.toString(), "item");
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
