package dk.ku.di.dms.vms.flightScheduler.test.AbortTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;

import java.net.http.HttpClient;
import java.util.ArrayList;

public class AbortStressTest
{
    public static boolean Run(HttpClient client) {
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

        // wait for components to come online
        System.console().readLine();
        System.out.println("TEST: Injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 50);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 100);

        // customer ids for unregistered customers
        var unregisteredCustomerIds = DataGenerator.CreateCustomers(50, 50);

        System.console().readLine();
        System.out.println(STR."TEST: submit 50 order flights, where every other of them fail");
        for (var i = 0; i < 50; i++)
        {
            // System.out.println(STR."TEST: ordering \{i/2} or \{i/2+1}");
            var customer = i % 2 == 0 ? unregisteredCustomerIds.get((i-1)/2) : customers.get(i/2);
            var flightSeat = flightSeats.get(i);

            Transactions.OrderFlight(client, customer, flightSeat);
        }

        System.console().readLine();

        var bookings = VmsEndpoints.GetBookings(client);

        var success = true;
        if (bookings.size() != 25)
        {
            System.out.println(STR."FAILURE (AbortStressTest): bookings=\{bookings.size()} != 25");
            success = false;
        }
        else {
            System.out.println(STR."SUCCESS (AbortStressTest): bookings=\{bookings.size()} != 25");
        }

        return success;
    }
}
