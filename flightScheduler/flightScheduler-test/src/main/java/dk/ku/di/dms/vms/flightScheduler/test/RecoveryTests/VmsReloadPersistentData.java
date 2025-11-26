package dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests;


import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;

import java.io.IOException;
import java.net.http.HttpClient;

public class VmsReloadPersistentData
{
    public static boolean Run(HttpClient client) throws IOException {

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
        System.out.println("TEST: injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 25);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 25);

        System.out.println(STR."TEST: sending order_flights TiDs [1,15)");
        for (var i = 0; i < 15; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        System.console().readLine();

        ComponentProcess.Kill("booking");

        System.console().readLine();

        ComponentProcess.StartVms("booking", true, 1);

        System.console().readLine();

        System.out.println(STR."TEST: sending order_flights TiDs [11,21)");
        for (var i = 15; i < 25; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        System.console().readLine();

        var bookings = VmsEndpoints.GetBookings(client);

        var success = true;
        if (bookings.size() != 20)
        {
            System.out.println(STR."FAILURE (VmsReloadPersistentData): bookings=\{bookings.size()} != 20");
            success = false;
        }
        else
        {
            System.out.println(STR."SUCCESS (VmsReloadPersistentData): bookings=\{bookings.size()} == 20");
        }
        return success;
    }
}
