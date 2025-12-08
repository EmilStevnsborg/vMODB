package dk.ku.di.dms.vms.tpcc.benchmark.ingestion;

import dk.ku.di.dms.vms.tpcc.benchmark.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenerator;
import dk.ku.di.dms.vms.tpcc.proxy.entities.District;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import static dk.ku.di.dms.vms.tpcc.benchmark.Constants.WAREHOUSE_PORT;

public class IngestionWorkerDistrict extends IngestionWorker
{
    public IngestionWorkerDistrict() {
        super();
    }

    @Override
    public void run()
    {
        // just one warehouse: # 1
        try
        {
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply("localhost", WAREHOUSE_PORT);
            for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                District district = DataGenerator.generateDistrict(d_id, 1);
                client.sendRequest("POST", district.toString(), "district");
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
