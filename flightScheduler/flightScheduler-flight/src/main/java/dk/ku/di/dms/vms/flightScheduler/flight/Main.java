package dk.ku.di.dms.vms.flightScheduler.flight;

import dk.ku.di.dms.vms.flightScheduler.flight.entities.FlightSeat;
import dk.ku.di.dms.vms.flightScheduler.flight.repositories.IFlightRepository;
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
                FLIGHT_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.flightScheduler.flight",
                        "dk.ku.di.dms.vms.flightScheduler.common"
                }, recoverable);
        return VmsApplication.build(options, (x,y) ->
                new FlightHttpHandler(x, (IFlightRepository) y.apply("flight_seats"))); // apply on y.apply({VmsTable Name})
    }

    private static class FlightHttpHandler extends DefaultHttpHandler
    {
        private final IFlightRepository repository;
        public FlightHttpHandler(ITransactionManager transactionManager,
                                 IFlightRepository repository){
            super(transactionManager);
            this.repository = repository;
        }

        @Override
        public void post(String uri, String payload)
        {
            String[] split = uri.split("/");
            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, false);

            if (split[split.length-1].equals("clear"))
            {
                System.out.println("DELETING ALL DATA");
                var flight_seats = repository.getAll();
                this.repository.deleteAll(flight_seats);
                return;
            }

            FlightSeat flightSeat = SERDES.deserialize(payload, FlightSeat.class);
            this.repository.upsert(flightSeat); // upsert: update + insert (if it exists, update, else insert)
        }

        // http://host/flight/{id}
        @Override
        public String getAsJson(String uri)
        {
            String[] split = uri.split("/");
            int flight_id = Integer.parseInt(split[split.length - 1]);
            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid,true);
            var flightSeats = this.repository.getAll();

            return flightSeats.stream()
                    .filter((fs -> fs.flight_id == flight_id))
                    .toList()
                    .toString();
        }
    }

}