package dk.ku.di.dms.vms.flightScheduler.test.Recovery;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
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
        VmsProcess.VmsProcessBuilder("customer").start();
        Util.Sleep(1000);
        System.out.println("\nCustomer process has started");

        // inject workload
        var customers = DataGenerator.GenerateCustomers(client, 50);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 50);

        // submit workload
        var half = customers.size()/2;
        for (var i = 0; i < half; i++)
        {
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));
        }

        // give time for workload to process
        Util.Sleep(500);

        // stop customer
        System.out.println("\nStopping customer process...");
        VmsProcess.KillCurrentVmsProcess("customer");

        // submit workload
        for (var i = half; i < customers.size(); i++)
        {
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));
        }

        // Restart customer
        VmsProcess.VmsProcessBuilder("customer").start();
        Util.Sleep(1000);
        System.out.println("\nCustomer process restarted");

        // wait for restart and recovery
        Util.Sleep(1000);

        // validate data (all flight seats should be allocated)




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
