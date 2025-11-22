package dk.ku.di.dms.vms.flightScheduler.payment;

import dk.ku.di.dms.vms.flightScheduler.payment.repositories.IPaymentRepository;
import dk.ku.di.dms.vms.flightScheduler.payment.repositories.IReimbursableBookingRepository;
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
            for (var arg : args)
            {
                var argSplit = arg.split("=");
                if (argSplit.length != 2) {
                    System.out.println(STR."invalid arg}");
                    continue;
                }

                var argName = argSplit[0];
                var argValue = argSplit[1];
                properties.setProperty(argName, argValue);
            }
        }
        VMS = buildVms(properties);
        VMS.start();
    }

    private static VmsApplication buildVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                PAYMENT_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.flightScheduler.payment",
                        "dk.ku.di.dms.vms.flightScheduler.common"
                });
        return VmsApplication.build(options,  (x,y) ->
                new PaymentHttpHandler(x, (IPaymentRepository) y.apply("payments"), (IReimbursableBookingRepository) y.apply("bookings_to_reimburse")));

        // y is an entity to table get function
    }

    private static class PaymentHttpHandler extends DefaultHttpHandler
    {
        private final IPaymentRepository paymentRepository;
        private final IReimbursableBookingRepository reimbursableBookingRepository;
        public PaymentHttpHandler(ITransactionManager transactionManager,
                                  IPaymentRepository paymentRepository,
                                  IReimbursableBookingRepository reimbursableBookingRepository){
            super(transactionManager);
            this.paymentRepository = paymentRepository;
            this.reimbursableBookingRepository = reimbursableBookingRepository;
        }

        @Override
        public void post(String uri, String payload) {

            String[] split = uri.split("/");
            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, false);

            if (split[split.length-1].equals("clear"))
            {
                System.out.println("DELETING ALL DATA");

                var payments = this.paymentRepository.getAll();
                this.paymentRepository.deleteAll(payments);
                var reimburses = this.reimbursableBookingRepository.getAll();
                this.reimbursableBookingRepository.deleteAll(reimburses);
            }
        }
        @Override
        public String getAsJson(String uri) {
            String[] split = uri.split("/");

            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, true);
            var payments = this.paymentRepository.getAllCommitted();
            return payments.toString();
        }
    }
}