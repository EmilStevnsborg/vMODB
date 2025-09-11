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

        var split = uri.split("/");
        TransactionInput.Event eventPayload;
        TransactionInput txInput;
        switch (split[split.length-1])
        {
            case "orderFlight":
                System.out.println("orderFlight: \n" + body);
                eventPayload = new TransactionInput.Event(ORDER_FLIGHT, body);
                txInput = new TransactionInput(ORDER_FLIGHT, eventPayload);
                this.coordinator.queueTransactionInput(txInput);
                break;
            case "payBooking":
                eventPayload = new TransactionInput.Event(PAY_BOOKING, body);
                txInput = new TransactionInput(PAY_BOOKING, eventPayload);
                this.coordinator.queueTransactionInput(txInput);
                break;
            case "cancelBooking":
                eventPayload = new TransactionInput.Event(CANCEL_BOOKING, body);
                txInput = new TransactionInput(CANCEL_BOOKING, eventPayload);
                this.coordinator.queueTransactionInput(txInput);
                break;
            case "reimburseBooking":
                eventPayload = new TransactionInput.Event(REIMBURSE_BOOKING, body);
                txInput = new TransactionInput(REIMBURSE_BOOKING, eventPayload);
                this.coordinator.queueTransactionInput(txInput);
                break;
            default:
                System.out.println("Invalid URI: " + uri);
                return;
        }
    }
}
