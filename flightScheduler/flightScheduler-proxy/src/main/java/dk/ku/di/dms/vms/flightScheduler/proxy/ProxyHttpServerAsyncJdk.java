package dk.ku.di.dms.vms.flightScheduler.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.infra.AbstractHttpHandler;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;

public final class ProxyHttpServerAsyncJdk extends AbstractHttpHandler implements IHttpHandler {

    public ProxyHttpServerAsyncJdk(Coordinator coordinator) {
        super(coordinator);
    }

    public void post(String uri, String body) {
//        System.out.println("\n################## NEW EVENT ###################\n");
//        System.out.println("\nProxyHttpServerAsyncJdk post");

        TransactionInput.Event eventPayload = new TransactionInput.Event(ORDER_FLIGHT, body);
        TransactionInput txInput = new TransactionInput(ORDER_FLIGHT, eventPayload);
        this.coordinator.queueTransactionInput(txInput);
    }
}
