package dk.ku.di.vms.hotelReservation.hotel;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.vms.hotelReservation.hotel.entities.Profile;
import dk.ku.di.vms.hotelReservation.hotel.entities.Rate;
import dk.ku.di.vms.hotelReservation.hotel.interfaces.HotelDistance;
import dk.ku.di.vms.hotelReservation.hotel.repositories.IProfileRepository;
import dk.ku.di.vms.hotelReservation.hotel.repositories.IRateRepository;

import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import static dk.ku.di.vms.hotelReservation.common.Constants.HOTEL_VMS_PORT;

public class Main
{
    private static VmsApplication VMS;
    public static void main(String[] args) throws Exception
    {
        Properties properties = ConfigUtils.loadProperties();

        if (args != null && args.length > 0) {
            var recoverable = Boolean.parseBoolean(args[0]);
            VMS = buildVms(properties, recoverable);
        } else {
            VMS = buildVms(properties, false);
        }
        VMS.start();
    }

    private static VmsApplication buildVms(Properties properties, boolean recoverable) throws Exception
    {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                HOTEL_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.flightScheduler.flight",
                        "dk.ku.di.dms.vms.flightScheduler.common"
                }, recoverable);
        return VmsApplication.build(options, (x,y) ->
                new HotelHttpHandler(x, (IProfileRepository) y.apply("profiles"), (IRateRepository) y.apply("rates")));
    }

    private static class HotelHttpHandler extends DefaultHttpHandler
    {
        private final IProfileRepository profileRepository;
        private final IRateRepository rateRepository;
        public HotelHttpHandler(ITransactionManager transactionManager,
                                IProfileRepository profileRepository,
                                IRateRepository rateRepository){
            super(transactionManager);
            this.profileRepository = profileRepository;
            this.rateRepository = rateRepository;
        }

        @Override
        public void post(String uri, String payload) {

            String[] split = uri.split("/");
            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, false);

            if (split[split.length-1].equals("rates"))
            {
                var rate = SERDES.deserialize(payload, Rate.class);

                // instead of insert directly, make sql command
                rateRepository.insert(rate);
            }
            else
            {
                var profile = SERDES.deserialize(payload, Profile.class);
                profileRepository.insert(profile);
            }

        }

        @Override
        public String getAsJson(String uri)
        {
            System.out.println("Get bookings");
            String[] split = uri.split("/");

            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, true);

            if (split[split.length-1].equals("recommendations"))
            {
                var metric = split[split.length-2];
                if (metric.equals("price"))
                {
                    var rates = rateRepository.getAll();
                    var minRate = rates.stream().min(Comparator.comparingDouble(r -> r.totalRate)).get().totalRate;
                    return rates
                            .stream()
                            .filter(r -> r.totalRate == minRate)
                            .map(r -> r.rate_id.hotel_id)
                            .distinct()
                            .toList()
                            .toString();
                }
                else if (metric.equals("rating"))
                {

                }
                else
                {
                    int lon = Integer.parseInt(split[split.length-3]);
                    int lat = Integer.parseInt(metric);
                    var distances = profileRepository.geoDistances(lon, lat);
                    var minDist = distances.stream().min(Comparator.comparingInt(HotelDistance::Distance)).get();

                    return distances
                            .stream()
                            .filter(d -> d.Distance() == minDist.Distance())
                            .toList()
                            .toString();
                }
            }
            return "";
        }
    }

}
