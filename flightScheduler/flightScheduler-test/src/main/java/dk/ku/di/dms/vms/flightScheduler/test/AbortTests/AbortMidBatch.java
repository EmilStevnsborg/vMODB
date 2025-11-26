package dk.ku.di.dms.vms.flightScheduler.test.AbortTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.DataInjection;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;

import java.net.http.HttpClient;
import java.util.List;

public class AbortMidBatch
{
    public static boolean Run(HttpClient client)
    {
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

        var customers = DataGenerator.GenerateCustomers(client, 29);
        var poorCustomer = new Customer(99, 0, "poor_customer");
        DataInjection.SendCustomer(client, poorCustomer);
        customers.add(poorCustomer);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 30);

        System.console().readLine();
        System.out.println(STR."TEST: sending order_flights with TIDs of [1-31)");
        for (var i = 0; i < customers.size(); i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        // wait for first orders
        System.console().readLine();

        var bookings = VmsEndpoints.GetBookings(client);
        var poorCustomerBooking = bookings.stream().filter(b -> b.customer_id==poorCustomer.customer_id).findFirst().get();
        if (poorCustomerBooking == null) return false;
        bookings.remove(poorCustomerBooking);

        System.console().readLine();
        System.out.println(STR."TEST: sending pay_bookings with TIDs of [31-41)");
        // send first batch and half of second batch
        for (var i = 0; i < 10; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        System.console().readLine();
        System.out.println(STR."TEST: sending pay_bookings with TIDs of [41-46)");
        for (var i = 10; i < 15; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        // send transaction that will fail and rest of the bookings
        System.console().readLine();
        System.out.println(STR."TEST: sending pay_booking to be aborted with TID of [46]");
        Transactions.PayBooking(client, poorCustomerBooking.booking_id, "VISA");

        System.console().readLine();
        System.out.println(STR."TEST: sending pay_bookings with TIDs of [47-51)");
        for (var i = 15; i < 19; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        // send third batch
        System.console().readLine();
        System.out.println(STR."TEST: sending pay_bookings with TIDs of [51-61)");
        for (var i = 19; i < 29; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        // wait for abort
        System.console().readLine();

        var updatedBookings = VmsEndpoints.GetBookings(client);
        var unpaidBookings = updatedBookings.stream().filter(b -> b.paid == 0).toList();
        var paidBookings = updatedBookings.stream().filter(b -> b.paid == 1).toList();

        boolean success = true;
        if (unpaidBookings.size() != 1) {
            System.out.println(STR."FAILURE (PayBookingAbort): unpaidBookings=\{unpaidBookings.size()} != 1");
            success = false;
        }
        else if (paidBookings.size() != 29) {
            System.out.println(STR."FAILURE (PayBookingAbort): paidBookings=\{paidBookings.size()} != 29");
            success = false;
        }
        else if (unpaidBookings.get(0).customer_id != poorCustomer.customer_id) {
            System.out.println(STR."FAILURE (PayBookingAbort): customer_id==\{unpaidBookings.get(0).customer_id} != poorCustomer==\{poorCustomer.customer_id}");
            success = false;
        }
        else {
            System.out.println("SUCCESS (PayBookingAbort): 29 paid bookings, and 1 unpaid booking for " +
                    STR."customer_id==\{unpaidBookings.get(0).customer_id}");
        }
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        return success;
    }
}
