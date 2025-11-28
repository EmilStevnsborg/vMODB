package dk.ku.di.dms.vms.flightScheduler.test.BasicTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;

import java.io.IOException;
import java.net.http.HttpClient;

public class LargeInjection
{
    public static boolean Run(HttpClient client) throws IOException {

        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 1, 1, 1,
                    1000, 500);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // wait for components to come online
        System.console().readLine();
        System.out.println("TEST: injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 1000);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 1000);

        System.out.println(STR."TEST: persisting data");
        VmsEndpoints.Commit(client);

        System.console().readLine();
        System.out.println("TEST: getting num flight seats");

        // Transactions.OrderFlight(client, customers.get(0), flightSeats.get(0));

        var numFlightSeats = VmsEndpoints.GetFlightSeatsCount(client);
        System.out.println(STR."numFlightSeats: \{numFlightSeats}");

        var success = false;
        return success;
    }
}
