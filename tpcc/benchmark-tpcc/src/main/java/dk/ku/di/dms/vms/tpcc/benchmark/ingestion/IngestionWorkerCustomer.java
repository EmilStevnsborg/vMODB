package dk.ku.di.dms.vms.tpcc.benchmark.ingestion;

import dk.ku.di.dms.vms.tpcc.benchmark.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenerator;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Customer;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Stock;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import static dk.ku.di.dms.vms.tpcc.benchmark.Constants.INVENTORY_PORT;
import static dk.ku.di.dms.vms.tpcc.benchmark.Constants.WAREHOUSE_PORT;

public class IngestionWorkerCustomer extends IngestionWorker
{
    public IngestionWorkerCustomer() {
        super();
    }

    @Override
    public void run()
    {
        // just one warehouse: # 1
        try
        {
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply("localhost", WAREHOUSE_PORT);
            for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++)
            {
                for (int c_id = 1; c_id <= TPCcConstants.NUM_CUST_PER_DIST; c_id++)
                {
                    Customer customer = DataGenerator.generateCustomer(c_id, d_id, 1);
                    client.sendRequest("POST", customer.toString(), "customer");
                }
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
