package dk.ku.di.dms.vms.flightScheduler.test.AbortTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.DataInjection;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;
import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;

import java.net.http.HttpClient;
import java.util.List;

public class AbortConcurrently
{
    public static boolean Run(HttpClient client)
    {
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        Util.Sleep(500);

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, Integer.MAX_VALUE, 10);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // wait for components to come online
        System.console().readLine();

        var customers = DataGenerator.GenerateCustomers(client, 19);
        var poorCustomer = new Customer(99, 0, "poor_customer");
        DataInjection.SendCustomer(client, poorCustomer);
        customers.add(poorCustomer);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 20);

        // 1st and 2nd batch
        for (var i = 0; i < 20; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        // wait for flight orders to complete
        System.console().readLine();

        // 3rd batch
        var bookings = VmsEndpoints.GetBookings(client);
        var poorCustomerBooking = bookings.stream().filter(b -> b.customer_id==poorCustomer.customer_id).findFirst().get();
        if (poorCustomerBooking == null) return false;
        bookings.remove(poorCustomerBooking);
        for (var i = 0; i < 10; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        // wait for bookings to be paid
        System.console().readLine();

        // 4th batch (two abort)
        for (var i = 10; i < 15; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");
        Transactions.PayBooking(client, poorCustomerBooking.booking_id, "VISA");
        Transactions.ReimburseBooking(client, bookings.get(19).booking_id);
        for (var i = 15; i < 18; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        // wait for abort procedure
        System.console().readLine();

        var updatedBookings = VmsEndpoints.GetBookings(client);
        var unpaidBookings = updatedBookings.stream().filter(b -> b.paid == 0).toList();
        var paidBookings = updatedBookings.stream().filter(b -> b.paid == 1).toList();


        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");

        return true;
    }
}
