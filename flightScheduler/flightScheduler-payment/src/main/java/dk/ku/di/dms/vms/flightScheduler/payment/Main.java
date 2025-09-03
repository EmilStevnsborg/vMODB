package dk.ku.di.dms.vms.flightScheduler.payment;

import dk.ku.di.dms.vms.flightScheduler.payment.infra.PaymentHttpHandler;
import dk.ku.di.dms.vms.flightScheduler.payment.repositories.IPaymentRepository;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;

public final class Main {

    public static void main(String[] args) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                PAYMENT_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.flightScheduler.payment",
                        "dk.ku.di.dms.vms.flightScheduler.common"
                });
        VmsApplication vms = VmsApplication.build(options,  (x,y) ->
                new PaymentHttpHandler(x, (IPaymentRepository) y.apply("payments")));
        vms.start();
    }

}