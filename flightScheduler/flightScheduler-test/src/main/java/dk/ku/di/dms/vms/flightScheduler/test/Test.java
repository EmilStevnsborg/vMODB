package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.test.models.FlightSeat;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Collections;

public class Test
{

    public static void GetUnpaidBookings(HttpClient client)
    {
        var unpaidBookings = DataRetrieval.GetUnpaidBookings(client);
        System.out.println(unpaidBookings);
    }

    public static void OrderFlights(HttpClient client)
    {
        // DATA INGESTION
        System.out.println("Injecting flight and customer data");

        var numSeatsPerFlight = 100;
        var flightSeats = DataCreation.CreateFlightSeats( numSeatsPerFlight, 0);
        flightSeats.forEach((flightSeat) -> DataInjection.SendFlightSeat(client, flightSeat));

        var numCustomers = 100;
        var customers = DataCreation.CreateCustomers(numCustomers);
        customers.forEach((customer) -> DataInjection.SendCustomer(client, customer));


        // ORDER FLIGHTS
        var numFlightOrders = 1;
        System.out.println(STR."Sending \{numFlightOrders} OrderFlightTransaction events");

        var customersCopy = new ArrayList<Customer>(customers);
        Collections.shuffle(customersCopy);
        var selectedCustomers = customersCopy.subList(0, numFlightOrders);

        var flightSeatsCopy = new ArrayList<FlightSeat>(flightSeats);
        Collections.shuffle(flightSeatsCopy);
        var selectedFlightSeats = flightSeatsCopy.subList(0, numFlightOrders);

        for(int i = 0; i < selectedCustomers.size(); i++)
        {
            Transactions.OrderFlight(client, selectedCustomers.get(i), selectedFlightSeats.get(i));
        }

        try
        {
            System.out.println("Stall getting the unpaid bookings ....");
            Thread.sleep(3000);
        } catch (InterruptedException e)
        {
            //
        }

        var unpaidBookings = DataRetrieval.GetUnpaidBookings(client);
    }
}
