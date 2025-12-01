package dk.ku.di.dms.vms.flightScheduler.booking;

import dk.ku.di.dms.vms.flightScheduler.booking.entities.Booking;
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
        }

        @Override
        public void post(String uri, String payload)
        {

            String[] split = uri.split("/");
            long lastTid = VMS.lastTidFinished();

            // persist data
            // save the data injected by adding them to keysToFlush (only happening when modifying via transaction task)
            if (split[split.length-1].equals("commit"))
            {
                System.out.println(STR."committing and checkpointing data up to \{lastTid} in booking");
                this.transactionManager.commit();
                this.transactionManager.checkpoint(lastTid);
                return;
            }

            this.transactionManager.beginTransaction(lastTid, 0, lastTid, false);

            Booking booking = SERDES.deserialize(payload, Booking.class);
            var bookingId = BookingService.booking_counter.incrementAndGet();
            booking.booking_id = bookingId;
            this.repository.upsert(booking);
        }


        // http://host/booking
        @Override
        public String getAsJson(String uri) {
            System.out.println("Get bookings");
            String[] split = uri.split("/");

            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, true);

            var bookings = this.repository.getAllCommitted();

            return bookings.toString();
        }
    }
}