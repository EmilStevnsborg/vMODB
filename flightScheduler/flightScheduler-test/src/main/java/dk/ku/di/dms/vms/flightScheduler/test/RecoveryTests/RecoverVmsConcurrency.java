package dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;

import java.io.IOException;
import java.net.http.HttpClient;

public class RecoverVmsConcurrency
{
    public static boolean Run(HttpClient client) throws IOException {
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 1, 1, 3,
                    Integer.MAX_VALUE, 5);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // wait for components to come online
        System.console().readLine();
        System.out.println("TEST: injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 40);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 40);

        // batch 1-3, should seal and commit at least two batches
        System.console().readLine();
        System.out.println(STR."TEST: sending order_flights with TIDs of [1-14)");
        for (var i = 0; i < 14; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        System.console().readLine();

        var success = true;
        return success;
    }
}
