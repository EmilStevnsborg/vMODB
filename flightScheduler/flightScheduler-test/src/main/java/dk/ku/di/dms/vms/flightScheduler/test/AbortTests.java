package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.test.models.FlightSeat;

import java.net.http.HttpClient;
import java.util.Collections;

public class AbortTests
{

    // the booking needs to be reversed if a customer has
    public static void CustomerCantAffordFlightSeat(HttpClient client)
    {
        // injecting data
        var dummyCustomers = DataGenerator.GenerateCustomers(client, 50);
        var flight_id = 0;
        var dummyFlightSeats = DataGenerator.GenerateFlightSeats(client, flight_id, 50);

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
        var bookings = Util.GetBookings(client);
        Collections.shuffle(bookings);

        // Pay bookings (will throw exception for poor customer)
        for (var booking : bookings)
        {
            Transactions.PayBooking(client, booking.booking_id, "VISA");
        }

        Util.Sleep(1000);

        // bookings
        var unpaidBookings = Util.GetBookings(client).stream().filter(booking -> booking.paid != 1).toList();


        // ASSERT RESULT
        if (unpaidBookings.size() != 1 && unpaidBookings.get(0).customer_id != poorCustomer.customer_id)
        {
            System.out.println("FAILED (CustomerCantAffordFlightSeat)");
        }

        System.out.println(STR."SUCCESS (CustomerCantAffordFlightSeat): only \{unpaidBookings.size()} unpaid booking for " +
                           STR."customer_id=\{unpaidBookings.get(0).customer_id}");
    }

    public void CancelBookingFailed(HttpClient client)
    {

    }
}
