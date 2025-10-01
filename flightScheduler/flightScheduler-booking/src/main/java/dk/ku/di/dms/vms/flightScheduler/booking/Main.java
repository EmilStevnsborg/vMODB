package dk.ku.di.dms.vms.flightScheduler.booking;

import dk.ku.di.dms.vms.flightScheduler.booking.repositories.IBookingRepository;
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

    public static void main(String[] ignoredArgs) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        VMS = buildVms(properties);
        VMS.start();
    }

    private static VmsApplication buildVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                BOOKING_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.flightScheduler.booking",
                        "dk.ku.di.dms.vms.flightScheduler.common"
                });
        return VmsApplication.build(options, (x,y) ->
                new BookingHttpHandler(x, (IBookingRepository) y.apply("bookings"))); // apply on y.apply({VmsTable Name})
    }
    private static class BookingHttpHandler extends DefaultHttpHandler
    {
        private final IBookingRepository repository;
        public BookingHttpHandler(ITransactionManager transactionManager,
                                  IBookingRepository repository){
            super(transactionManager);
            this.repository = repository;
            System.out.println(STR."\nRepo is \{repository}\n");
        }

        // http://host/booking
        @Override
        public String getAsJson(String uri) {
            System.out.println("Get bookings");
            String[] split = uri.split("/");
            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, true);
            var bookings = this.repository.getAll();

            // return the (unpaid) booking ids
            if (split[split.length-1].equals("unpaid")){
                return bookings.stream()
                        .filter((booking -> booking.paid == 0))
                        .toList()
                        .toString();
            }
            return bookings.toString();
        }
    }
}