package dk.ku.di.dms.vms.flightScheduler.test.AbortTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.DataInjection;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;
import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.List;

public class AbortConcurrently
{
    public static boolean Run(HttpClient client) throws IOException
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
        System.out.println("TEST: injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 19);
        var poorCustomer = new Customer(99, 0, "poor_customer");
        DataInjection.SendCustomer(client, poorCustomer);
        customers.add(poorCustomer);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 20);

        System.console().readLine();
        System.out.println("TEST: sending order_flight with TIDs of [1-21)");
        // 1st and 2nd batch
        for (var i = 0; i < 20; i++)
            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));

        // wait for flight orders to complete
        System.console().readLine();
        System.out.println("TEST: sending pay_booking with TIDs of [21-31)");

        // 3rd batch
        var bookings = VmsEndpoints.GetBookings(client);
        var poorCustomerBooking = bookings.stream().filter(b -> b.customer_id==poorCustomer.customer_id).findFirst().get();
        if (poorCustomerBooking == null) return false;
        bookings.remove(poorCustomerBooking);
        for (var i = 0; i < 10; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        // wait for bookings to be paid
        System.console().readLine();
        System.out.println("TEST: sending pay_booking with TIDs of [31-36)");

        // 4th batch (two abort)
        for (var i = 10; i < 15; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        System.console().readLine();

        System.out.println("TEST: sending FAILURE pay_booking with TID of [36]");
        Transactions.PayBooking(client, poorCustomerBooking.booking_id, "VISA");

        //System.console().readLine();

        System.out.println("TEST: sending FAILURE reimburse_booking with TID of [37]");
        var bookingToReimburse = bookings.get(18);
        Transactions.ReimburseBooking(client, bookingToReimburse.booking_id);

        System.console().readLine();
        System.out.println("TEST: sending pay_booking with TIDs of [38-41)");
        for (var i = 15; i < 18; i++)
            Transactions.PayBooking(client, bookings.get(i).booking_id, "VISA");

        // wait for abort procedure
        System.console().readLine();

        var updatedBookings = VmsEndpoints.GetBookings(client);
        var updatedCustomers = VmsEndpoints.GetCustomers(client);

        var unpaidBookings = updatedBookings.stream().filter(b -> b.paid == 0).toList();
        var reimbursementCId = bookingToReimburse.customer_id;
        var beforeNOTReimbursement = customers.stream().filter(c -> c.customer_id == reimbursementCId).findFirst().get();
        var afterNOTReimbursement = updatedCustomers.stream().filter(c -> c.customer_id == reimbursementCId).findFirst().get();


        var success = true;
        if (unpaidBookings.size() != 2)
        {
            System.out.println(STR."FAILURE (AbortConcurrently): unpaidBookings.size()=\{unpaidBookings.size()} != 2");
            success = false;
        }
        else if (beforeNOTReimbursement.money != afterNOTReimbursement.money)
        {
            System.out.println(STR."FAILURE (AbortConcurrently): beforeNOTReimbursement.money=\{beforeNOTReimbursement.money} " +
                               STR."!= afterNOTReimbursement.money=\{afterNOTReimbursement.money}");
            success = false;
        }
        else
        {
            System.out.println(STR."SUCCESS (AbortConcurrently): unpaidBookings.size()=\{unpaidBookings.size()} == 2 " +
                               STR."FAILURE (AbortConcurrently): beforeNOTReimbursement.money=\{beforeNOTReimbursement.money} " +
                               STR."!= afterNOTReimbursement.money=\{afterNOTReimbursement.money}");
        }

        return success;
    }
}
