package dk.ku.di.dms.vms.flightScheduler.test.AbortTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.DataInjection;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsProcess;
import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.test.models.FlightSeat;

import java.net.http.HttpClient;
import java.util.Collections;

public class AbortTests
{

    // the booking needs to be reversed if a customer has
    public static void CustomerCantAffordFlightSeat(HttpClient client)
    {

        Util.Sleep(1000);

        // injecting data
        var dummyCustomers = DataGenerator.GenerateCustomers(client, 50);
        var dummyFlightSeats = DataGenerator.GenerateFlightSeats(client, 0, 50);

        // poor customer
        var poorCustomer = new Customer(99, 0, "poor_customer");
        var poorCustomerFlightSeat = new FlightSeat(0, "poor_customer_seat");
        DataInjection.SendCustomer(client, poorCustomer);
        DataInjection.SendFlightSeat(client, poorCustomerFlightSeat);
        dummyCustomers.add(poorCustomer);
        dummyFlightSeats.add(poorCustomerFlightSeat);

        // Order flights
        for (var i = 0; i < dummyCustomers.size(); i++)
        {
            Transactions.OrderFlight(client, dummyCustomers.get(i), dummyFlightSeats.get(i));
        }

        Util.Sleep(1000);

        // bookings
        var bookings = VmsEndpoints.GetBookings(client);
        Collections.shuffle(bookings);

        // Pay bookings (will throw exception for poor customer)
        for (var booking : bookings)
        {
            Transactions.PayBooking(client, booking.booking_id, "VISA");
        }

        Util.Sleep(1000);

        // bookings
        var unpaidBookings = VmsEndpoints.GetBookings(client).stream().filter(booking -> booking.paid != 1).toList();

        // customers
        var customers = VmsEndpoints.GetCustomers(client);
        var poorCustomerGet = customers.stream().filter(c -> c.customer_id == poorCustomer.customer_id).toList().get(0);


        // ASSERT RESULT
        if (poorCustomerGet.money != poorCustomer.money ||
            unpaidBookings.size() != 1 ||
            unpaidBookings.get(0).customer_id != poorCustomer.customer_id
        )
        {
            System.out.println(STR."FAILED (CustomerCantAffordFlightSeat): " +
                               STR."poorCustomerGet.money=\{poorCustomerGet.money} != \{poorCustomer.money}=poorCustomer.money, " +
                               STR."unpaid booking = \{unpaidBookings.size()} != 1"); // aborted pay booking
            return;
        }

        System.out.println(STR."SUCCESS (CustomerCantAffordFlightSeat): only \{unpaidBookings.size()} unpaid booking for " +
                           STR."customer_id=\{unpaidBookings.get(0).customer_id}");
    }

    public void CancelBookingFailed(HttpClient client)
    {


    }
}
