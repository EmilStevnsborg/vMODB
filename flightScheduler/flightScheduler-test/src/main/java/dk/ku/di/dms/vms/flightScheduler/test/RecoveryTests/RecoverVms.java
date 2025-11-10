package dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;
import dk.ku.di.dms.vms.flightScheduler.test.models.Booking;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;

public class RecoverVms
{
    public static boolean Run(HttpClient client) throws IOException {

        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, Integer.MAX_VALUE, 10);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // wait for components to come online
        System.console().readLine();

        var customers = DataGenerator.GenerateCustomers(client, 40);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 40);

        // 1st batch
        for (var i = 0; i < 10; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        System.console().readLine();

        // 2nd batch
        for (var i = 10; i < 20; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        // wait for flight orders to commit
        System.console().readLine();

        var bookings = VmsEndpoints.GetBookings(client);
        System.out.println(STR."There are \{bookings.size()} registered bookings");

        // 3rd batch (abort flight orders)
        for (var i = 20; i < 25; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        System.console().readLine();
        for (var i = 0; i < 4; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        // wait for flight orders to complete
        ComponentProcess.Kill("flight");
        System.out.println("Submitting rest of batch 3");
        Transactions.PayBooking(client, bookings.get(4).booking_id, "VISA"); // finish batch
        System.console().readLine();

        // 4th batch
        for (var i = 25; i < 30; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));
        for (var i = 5; i < 15; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        // wait for batch to commit
        System.console().readLine();

        ComponentProcess.StartVms("flight", true);

        // wait for VMS to recover
        System.console().readLine();

        // 5th batch
        for (var i = 30; i < 40; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        // wait for batch to commit
        System.console().readLine();

        var updatedBookings = VmsEndpoints.GetBookings(client);
        var unpaidBookings = updatedBookings.stream().filter(b -> b.paid == 1).toList();

        var updatedFlightSeats = VmsEndpoints.GetFlightSeats(client, 0);
        var numOccupiedFlightSeats = updatedFlightSeats.stream().filter(fs -> fs.occupied == 1).count();

        var success = numOccupiedFlightSeats == 30 && unpaidBookings.size() == 15;
        if (numOccupiedFlightSeats != 30)
        {
            System.out.println(STR."FAILURE (RecoverVms): numOccupiedFlightSeats=\{numOccupiedFlightSeats} != 30");
        }
        else if (unpaidBookings.size() != 15)
        {
            System.out.println(STR."FAILURE (RecoverVms): numOccupiedFlightSeats=\{numOccupiedFlightSeats} != 30");
        }
        else
        {
            System.out.println(STR."SUCCESS (RecoverVms)");
        }

        return success;
    }
}
