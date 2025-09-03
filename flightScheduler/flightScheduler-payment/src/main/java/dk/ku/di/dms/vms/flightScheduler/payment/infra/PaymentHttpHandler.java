package dk.ku.di.dms.vms.flightScheduler.payment.infra;

import dk.ku.di.dms.vms.flightScheduler.payment.repositories.IPaymentRepository;
import dk.ku.di.dms.vms.flightScheduler.payment.repositories.IReimbursableBookingRepository;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;

public class PaymentHttpHandler extends DefaultHttpHandler
{
    private final IPaymentRepository paymentRepository;
    public PaymentHttpHandler(ITransactionManager transactionManager,
                              IPaymentRepository paymentRepository){
        super(transactionManager);
        this.paymentRepository = paymentRepository;
        System.out.println(STR."\nRepo is \{paymentRepository}\n");
    }

    // http://host/booking
    @Override
    public String getAsJson(String uri) {
        String[] split = uri.split("/");
        this.transactionManager.beginTransaction(0, 0, 0,true);
        var payments = this.paymentRepository.getAll();
        return payments.toString();
    }
}
