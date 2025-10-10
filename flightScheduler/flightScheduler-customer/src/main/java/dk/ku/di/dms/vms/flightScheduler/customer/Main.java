package dk.ku.di.dms.vms.flightScheduler.customer;

import dk.ku.di.dms.vms.flightScheduler.customer.entities.Customer;
import dk.ku.di.dms.vms.flightScheduler.customer.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

import java.util.Properties;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;

public final class Main {
    private static VmsApplication VMS;
    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        Properties properties = ConfigUtils.loadProperties();

        if (args != null && args.length > 0) {
            var recoverable = Boolean.parseBoolean(args[0]);
            VMS = buildVms(properties, recoverable);
        } else {
            VMS = buildVms(properties, false);
        }
        VMS.start();
    }

    private static VmsApplication buildVms(Properties properties, boolean recoverable) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                CUSTOMER_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.flightScheduler.customer",
                        "dk.ku.di.dms.vms.flightScheduler.common"
                }, recoverable);
        return VmsApplication.build(options, (x,y) ->
                new CustomerHttpHandler(x, (ICustomerRepository) y.apply("customers"))); // apply on y.apply({VmsTable Name})
    }
    private static class CustomerHttpHandler extends DefaultHttpHandler
    {
        private final ICustomerRepository repository;
        public CustomerHttpHandler(ITransactionManager transactionManager,
                                   ICustomerRepository repository){
            super(transactionManager);
            this.repository = repository;
            System.out.println(STR."\nRepo is \{repository}\n");
        }

        @Override
        public void post(String uri, String payload)
        {
//        System.out.println(STR."\nPosting \{payload}\n");
            Customer customer = SERDES.deserialize(payload, Customer.class);
            this.transactionManager.beginTransaction(0, 0, 0, false);
            this.repository.upsert(customer);
        }

        // http://host/customer/{id}
        @Override
        public String getAsJson(String uri) {
            System.out.println("Get customers");
            String[] split = uri.split("/");
            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, true);
            if (split[split.length - 1].equals("customer")) {
                var customers = this.repository.getAll();
                return customers.toString();
            }
            else {
                int customer_id = Integer.parseInt(split[split.length - 1]);
                Customer customer = this.repository.lookupByKey(customer_id);
                return customer.toString();
            }
        }
    }
}