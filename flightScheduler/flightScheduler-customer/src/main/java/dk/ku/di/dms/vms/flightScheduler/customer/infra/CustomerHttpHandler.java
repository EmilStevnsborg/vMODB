package dk.ku.di.dms.vms.flightScheduler.customer.infra;

import dk.ku.di.dms.vms.flightScheduler.customer.entities.Customer;
import dk.ku.di.dms.vms.flightScheduler.customer.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;

public class CustomerHttpHandler extends DefaultHttpHandler
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
        this.repository.upsert(customer); // upsert: update + insert (if it exists, update, else insert)
    }

    // http://host/customer/{id}
    @Override
    public String getAsJson(String uri) {
        String[] split = uri.split("/");
        int customer_id = Integer.parseInt(split[split.length - 1]);
        this.transactionManager.beginTransaction(0, 0, 0,true);
        Customer customer = this.repository.lookupByKey(customer_id);
        return customer.toString();
    }
}
