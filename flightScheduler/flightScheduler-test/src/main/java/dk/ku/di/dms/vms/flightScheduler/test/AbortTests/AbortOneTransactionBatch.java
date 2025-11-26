package dk.ku.di.dms.vms.flightScheduler.test.AbortTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.DataInjection;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;

import java.net.http.HttpClient;

public class AbortOneTransactionBatch
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

        var customers = DataGenerator.GenerateCustomers(client, 30);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 30);

        System.console().readLine();
        System.out.println(STR."TEST: sending order_flights with TIDs of [1-31)");
        for (var i = 0; i < customers.size(); i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        var success = true;
        return success;
    }
}
