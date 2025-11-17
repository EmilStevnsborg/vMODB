package dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;

import java.io.IOException;
import java.net.http.HttpClient;

public class RecoverCoordinator
{
    public static boolean Run(HttpClient client)throws IOException
    {
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 1, 1, 1,
                    Integer.MAX_VALUE, 10);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        System.console().readLine();
        System.out.println("TEST: injecting data");

        // inject workload
        var customers = DataGenerator.GenerateCustomers(client, 20);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 20);

        System.console().readLine();
        System.out.println("TEST: sending order_flight with TIDs of [1-21)");

        for (var i = 0; i < flightSeats.size(); i++) {
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));}

        System.console().readLine();
        var allBookings = VmsEndpoints.GetBookings(client);
        System.out.println(STR."There are \{allBookings.size()} registered bookings");

        System.console().readLine();
        System.out.println("TEST: sending pay_booking with TIDs of [21-26)");

        for (var i = 0; i < 5; i++) {
            Transactions.PayBooking(client, allBookings.get(i).booking_id, "VISA");
        }

        System.console().readLine();
        ComponentProcess.Kill("proxy");
        System.out.println("\nCoordinator crashed...");

        System.console().readLine();

        ComponentProcess.StartProxy(true, 1, 1, 1,
                Integer.MAX_VALUE, 10);
        System.out.println("\nCoordinator restarted...");

        System.console().readLine();
        System.out.println("TEST: sending pay_booking with TIDs of [21-31)");

        for (var i = 5; i < 15; i++) {
            Transactions.PayBooking(client, allBookings.get(i).booking_id, "VISA");
        }

        System.console().readLine();
        var updatedBookings = VmsEndpoints.GetBookings(client);
        var unpaidBookings = updatedBookings.stream().filter(b -> b.paid == 0).toList();
        var paidBookings = updatedBookings.stream().filter(b -> b.paid == 1).toList();

        var success = true;
        if (unpaidBookings.size() != 10)
        {
            System.out.println(STR."FAILURE (RecoverCoordinator): unpaidBookings.size()=\{unpaidBookings.size()} != 10");
            success = false;
        }
        else if (paidBookings.size() != 10)
        {
            System.out.println(STR."FAILURE (RecoverCoordinator): paidBookings.size()=\{paidBookings.size()} != 10");
            success = false;
        }
        else
        {
            System.out.println(STR."SUCCESS (RecoverVms): unpaidBookings=\{unpaidBookings.size()} == 10, " +
                    STR."paidBookings=\{paidBookings.size()} == 10");
        }
        return success;
    }
}
