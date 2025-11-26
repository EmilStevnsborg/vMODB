package dk.ku.di.dms.vms.flightScheduler.test.BasicTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.DataInjection;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;
import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;

import java.net.http.HttpClient;

public class BasicTest
{
    public static boolean Run(HttpClient client) {
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");

        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 2, 2, 5,
                    500, 500);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // wait for components to come online
        System.console().readLine();
        System.out.println("TEST: Injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 50);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 50);

        System.console().readLine();
        System.out.println(STR."TEST: sending order_flights with TIDs of [1-51)");
        for (var i = 0; i < 10; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        // wait for first orders
        System.console().readLine();

        var customers_injected = VmsEndpoints.GetCustomers(client);
        var flightSeats_injected = VmsEndpoints.GetFlightSeats(client, 0);
        var bookings = VmsEndpoints.GetBookings(client);

        System.out.println(STR."customers: \{customers_injected.size()}, flightSeats_injected: \{flightSeats_injected.size()}, bookings: \{bookings.size()}");

        System.console().readLine();

        var success = false;
        return success;
    }
}
