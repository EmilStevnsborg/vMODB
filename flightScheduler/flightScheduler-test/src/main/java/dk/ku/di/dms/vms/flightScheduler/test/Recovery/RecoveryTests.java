package dk.ku.di.dms.vms.flightScheduler.test.Recovery;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsProcess;

import java.io.IOException;
import java.net.http.HttpClient;

public class RecoveryTests
{
    private HttpClient client;
    public RecoveryTests(HttpClient client)
    {
        this.client = client;
    }

    // All injected customers need to have paid for a flight
    public static void CustomerCrash(HttpClient client) throws IOException
    {
        VmsProcess.KillCurrentVmsProcess("customer");
        VmsProcess.VmsProcessBuilder("customer", false).start();
        Util.Sleep(1000);
        System.out.println("\nCustomer process has started");

        System.console().readLine();

        // inject workload
        var customers = DataGenerator.GenerateCustomers(client, 50);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 50);

        Util.Sleep(500);

        // submit workload
        var half = customers.size()/2;
        for (var i = 0; i < half; i++)
        {
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));
        }

        // give time for workload to process
        Util.Sleep(500);

        // stop customer
        System.console().readLine();
        System.out.println("\nStopping customer process...");
        VmsProcess.KillCurrentVmsProcess("customer");


        // submit workload
        System.console().readLine();
        System.out.println("\nSubmitting second workload while customer is down");
        for (var i = half; i < half+1; i++)
        {
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));
        }


        // Restart customer
        System.console().readLine();
        System.out.println("\nRestarting customer process...");
        VmsProcess.VmsProcessBuilder("customer", true).start();


        // wait for restart and recovery
        System.console().readLine();
        System.out.println("\nGetting validation data...");
        var flightSeatsGet = VmsEndpoints.GetFlightSeats(client, 0);
        var customersGet = VmsEndpoints.GetCustomers(client);

        var occupiedFlightSeats = flightSeatsGet.stream().filter(fs -> fs.occupied == 1).count();
        var customersSeatsReservedSorted = customersGet.stream().map(c -> c.seat_number).sorted().toList();
        var flightSeatsInjectedSorted = flightSeats.stream().map(fs -> fs.seat_number).sorted().toList();

        if (occupiedFlightSeats != 50)
        {
            System.out.println(STR."FAILURE (CustomerCrash): occupiedFlightSeats=\{occupiedFlightSeats}!=50");
        }
        if (!customersSeatsReservedSorted.equals(flightSeatsInjectedSorted))
        {
            System.out.println(STR."FAILURE (CustomerCrash): reserved seats not equal to all available seats");
        }
        else
        {
            System.out.println(STR."SUCCESS (CCustomerCrash)");
        }

        // stop process final
        System.console().readLine();
        System.out.println("\nStopping customer process final...");

        // Final kill
        VmsProcess.KillCurrentVmsProcess("customer");
    }

    public static void CoordinatorCrash(HttpClient client)
    {

        VmsProcess.KillCurrentVmsProcess("proxy");
    }


    public static void ContinuousWorkload(HttpClient client)
    {

    }
}
