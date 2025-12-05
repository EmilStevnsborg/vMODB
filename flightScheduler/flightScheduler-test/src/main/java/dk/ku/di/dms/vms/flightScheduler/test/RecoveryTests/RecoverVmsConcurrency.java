package dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;

import java.io.IOException;
import java.net.http.HttpClient;

public class RecoverVmsConcurrency {
    public static boolean Run(HttpClient client) throws IOException {
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 1, 1, 2,
                    Integer.MAX_VALUE, 1);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        System.console().readLine();
        System.out.println("TEST: injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 40);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 40);

        System.console().readLine();
        System.out.println("TEST: committing injecting data");
        VmsEndpoints.Commit(client);

        // 1st and 2nd batch
        System.console().readLine();
        System.out.println(STR."TEST: sending order_flights with TIDs of [1-21)");
        for (var i = 0; i < 20; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        System.console().readLine();

        // wait for flight orders to complete
        ComponentProcess.Kill("flight");

        System.console().readLine();
        System.out.println(STR."TEST: sending order_flights but being denied");
        for (var i = 0; i < 10; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        System.console().readLine();

        ComponentProcess.StartVms("flight", true, 1);
        System.out.println(STR."Flight is back online");

        System.console().readLine();
        System.out.println(STR."TEST: sending order_flights but being denied");
        for (var i = 0; i < 10; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        System.console().readLine();

        var bookings = VmsEndpoints.GetBookings(client);

        var success = true;
        var unpaidBookings = bookings.stream().filter(b -> b.paid == 0).toList();
        if (unpaidBookings.size() != 10)
        {
            System.out.println(STR."FAILURE (RecoverVms): unpaidBookings=\{unpaidBookings.size()} != 20");
            success = false;
        }
        else
        {
            System.out.println(STR."SUCCESS (RecoverVms): unpaidBookings=\{unpaidBookings.size()} == 10");
        }

        return success;
    }

}
