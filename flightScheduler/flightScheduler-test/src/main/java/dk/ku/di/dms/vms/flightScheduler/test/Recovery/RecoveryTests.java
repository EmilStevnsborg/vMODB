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

    private static void killComponents()
    {
        VmsProcess.KillCurrentVmsProcess("customer");
        VmsProcess.KillCurrentVmsProcess("flight");
        VmsProcess.KillCurrentVmsProcess("payment");
        VmsProcess.KillCurrentVmsProcess("booking");
        VmsProcess.KillCurrentVmsProcess("proxy");
    }
    private static void startComponents() throws IOException
    {
        VmsProcess.VmsProcessBuilder("customer", false).start();
        VmsProcess.VmsProcessBuilder("flight", false).start();
        VmsProcess.VmsProcessBuilder("payment", false).start();
        VmsProcess.VmsProcessBuilder("booking", false).start();
        VmsProcess.VmsProcessBuilder("proxy", false).start();
    }

    public static void SimpleReconnection(HttpClient client) throws IOException
    {
        VmsProcess.KillCurrentVmsProcess("customer");
        VmsProcess.VmsProcessBuilder("customer", false).start();

        System.console().readLine();
        System.out.println("Stopping customer");
        VmsProcess.KillCurrentVmsProcess("customer");

        System.console().readLine();
        System.out.println("Restarting customer");
        VmsProcess.VmsProcessBuilder("customer", true).start();

        System.console().readLine();
        System.out.println("Final stop of customer");
        VmsProcess.KillCurrentVmsProcess("customer");

    }

    public static void VMSCrash(HttpClient client) throws IOException
    {
        killComponents();
        startComponents();
        System.console().readLine();
        Util.Sleep(1000);

        System.out.println("Injecting data into customer and flight...");
        var customers = DataGenerator.GenerateCustomers(client, 50);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 50);
        Util.Sleep(500);

        System.out.println("Ordering flights...");
        for (var i = 0; i < 25; i++) { Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i)); }
        Util.Sleep(500);

        System.out.println("Customer crashing...");
        VmsProcess.KillCurrentVmsProcess("customer");

        System.out.println("Ordering flights...");
        for (var i = 25; i < 50; i++) { Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i)); }
        Util.Sleep(500);

        System.out.println("Customer restarting...");
        VmsProcess.VmsProcessBuilder("customer", false).start();
        Util.Sleep(1000);

        var customersSeatsReserved = VmsEndpoints.GetCustomers(client).stream().map(c -> c.seat_number).distinct().toList();

        killComponents();

        System.out.println(STR."There are \{flightSeats.size()} distinct flight seats, and \{customersSeatsReserved.size()} distinct customer seats");
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
        for (var i = half; i < customers.size(); i++)
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
        var customersSeatsReserved = VmsEndpoints.GetCustomers(client).stream().map(c -> c.seat_number).distinct().toList();




        // VERDICT (should be 50, but only reads getAll from memory)
        if (customersSeatsReserved.size() != 50)
        {
            System.out.println(STR."FAILURE (CustomerCrash): distinct seats reserved=\{customersSeatsReserved.size()}!=50");
        }
        else {
            System.out.println(STR."SUCCESS (CustomerCrash) distinct seats reserved=\{customersSeatsReserved.size()}==50");
        }


        // stop process final
        System.console().readLine();
        System.out.println("\nStopping customer process final...");

        // Final kill
        VmsProcess.KillCurrentVmsProcess("customer");
    }

    public static void CoordinatorCrash(HttpClient client) throws IOException
    {
        VmsProcess.KillCurrentVmsProcess("proxy");
        VmsProcess.VmsProcessBuilder("proxy", false).start();
        Util.Sleep(1000);
        System.out.println("\nCoordinator process has started");

        System.console().readLine();
        System.out.println("\nInjecting workload...");

        // inject workload
        var customers = DataGenerator.GenerateCustomers(client, 10);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 10);

        System.console().readLine();
        System.out.println("\nOrdering flights...");

        for (var i = 0; i < flightSeats.size(); i++) {Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));}

        System.console().readLine();
        System.out.println("\nPay bookings...");

        var allBookings = VmsEndpoints.GetBookings(client);
        var half = allBookings.size()/2;

        for (var i = 0; i < half; i++) {
            Transactions.PayBooking(client, allBookings.get(i).booking_id, "VISA");
        }
        VmsProcess.KillCurrentVmsProcess("proxy");
        System.out.println("\nCoordinator crashed...");
        System.console().readLine();

        VmsProcess.VmsProcessBuilder("proxy", true).start();
        Util.Sleep(1000);
        System.out.println("\nCoordinator restarted...");

        System.console().readLine();
        System.out.println("\nPay second half of the bookings...");

        for (var i = half; i < allBookings.size(); i++) {
            Transactions.PayBooking(client, allBookings.get(i).booking_id, "VISA");
        }

        System.console().readLine();
        System.out.println("\nGetting validation data...");

        var unpaidBookings = VmsEndpoints.GetBookings(client).stream().filter(b -> b.paid == 1).toList();
        System.out.println(STR."(CoordinatorCrash): unpaid bookings count=\{unpaidBookings.size()}");
        VmsProcess.KillCurrentVmsProcess("proxy");
    }


    public static void ContinuousWorkload(HttpClient client)
    {

    }
}
