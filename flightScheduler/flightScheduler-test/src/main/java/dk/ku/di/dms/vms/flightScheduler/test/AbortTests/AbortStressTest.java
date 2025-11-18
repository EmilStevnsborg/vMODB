package dk.ku.di.dms.vms.flightScheduler.test.AbortTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;

import java.net.http.HttpClient;

public class AbortStressTest
{
    public static boolean Run(HttpClient client) {
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");

        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 1, 1, 1,
                    Integer.MAX_VALUE, 20);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // wait for components to come online
        System.console().readLine();
        System.out.println("TEST: Injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 20);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 100);

        System.console().readLine();
        System.out.println(STR."TEST: sending order_flights with TIDs of [1-11)");
        for (var i = 0; i < customers.size(); i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));


        System.console().readLine();

        var bookings = VmsEndpoints.GetBookings(client);

        var success = true;
        return success;
    }
}
