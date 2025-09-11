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

    public static void Scenario1(HttpClient client)
    {
        // First inject some data
        var numSeatsPerFlight = 100;
        var flightSeats = DataCreation.CreateFlightSeats( numSeatsPerFlight, 0);
        flightSeats.forEach((flightSeat) -> DataInjection.SendFlightSeat(client, flightSeat));

        var numCustomers = 100;
        var customers = DataCreation.CreateCustomers(numCustomers);
        customers.forEach((customer) -> DataInjection.SendCustomer(client, customer));

        ///////////////////////////////////////////////////////////////////////////////
        /////////////////// Create some bookings through OrderFlight //////////////////
        ///////////////////////////////////////////////////////////////////////////////

        var numBookings = 30;

        var customersCopy = new ArrayList<Customer>(customers);
        Collections.shuffle(customersCopy);

        var flightSeatsCopy = new ArrayList<FlightSeat>(flightSeats);
        Collections.shuffle(flightSeatsCopy);

        for(int i = 0; i < numBookings; i++)
        {
            Transactions.OrderFlight(client, customersCopy.get(i), flightSeatsCopy.get(i));
        }

        // Wait a bit for the system to process the data
        try
        {
            System.out.println("Stall after ordering flights ....");
            Thread.sleep(3000);
        } catch (InterruptedException e){}

        ///////////////////////////////////////////////////////////////////////////////
        ////////////////// Pay the bookings to create payment records /////////////////
        ///////////////////////////////////////////////////////////////////////////////

        var numBookingsToPay = 10;
        var unpaidBookings = DataRetrieval.GetUnpaidBookings(client);
        System.out.println(unpaidBookings);
        System.out.println(unpaidBookings.size()); // should be 30

        for (int i = 0; i < numBookingsToPay; i++)
        {
            Transactions.PayBooking(client, unpaidBookings.get(i), "VISA");
        }
        var reimbursableBookings = unpaidBookings.subList(0, numBookingsToPay);
        // Wait a bit for the system to process the data
        try
        {
            System.out.println("Stall after paying bookings ....");
            Thread.sleep(3000);
        } catch (InterruptedException e){}
        unpaidBookings = DataRetrieval.GetUnpaidBookings(client);
        System.out.println(unpaidBookings);
        System.out.println(unpaidBookings.size()); // should be 20

        ///////////////////////////////////////////////////////////////////////////////
        ///////// Cancel the bookings to create reimbursable booking records //////////
        ///////////////////////////////////////////////////////////////////////////////

        var numReimbursableBookings = 10;
        for (int i = 0; i < numReimbursableBookings; i++)
        {
            Transactions.CancelBooking(client, reimbursableBookings.get(i));
        }
        // Wait a bit for the system to process the data
        try
        {
            System.out.println("Stall after cancelling bookings ....");
            Thread.sleep(3000);
        } catch (InterruptedException e){}
        unpaidBookings = DataRetrieval.GetUnpaidBookings(client);
        System.out.println(unpaidBookings);
        System.out.println(unpaidBookings.size()); // should be 20

        ///////////////////////////////////////////////////////////////////////////////
        ///////////////////////////////// Start batch /////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////

        // 70 OrderFlights
        var numOrderFlights = 70;
        for(int i = numBookings; i < numOrderFlights+numBookings; i++)
        {
            Transactions.OrderFlight(client, customersCopy.get(i), flightSeatsCopy.get(i));
        }

        // 20 PayBookings
        for (var unpaidBooking : unpaidBookings)
        {
            Transactions.PayBooking(client, unpaidBooking, "MASTERCARD");
        }

        // 10 ReimburseBooking
        for (var reimbursableBooking : reimbursableBookings)
        {
            Transactions.ReimburseBooking(client, reimbursableBooking);
        }
    }

    public static void OrderFlights(HttpClient client)
    {
        // DATA INGESTION
        System.out.println("Injecting flight and customer data");

        var numSeatsPerFlight = 5;
        var flightSeats = DataCreation.CreateFlightSeats( numSeatsPerFlight, 0);
        flightSeats.forEach((flightSeat) -> DataInjection.SendFlightSeat(client, flightSeat));

        var numCustomers = 5;
        var customers = DataCreation.CreateCustomers(numCustomers);
        customers.forEach((customer) -> DataInjection.SendCustomer(client, customer));


        // ORDER FLIGHTS
        var numFlightOrders = 5;
        System.out.println(STR."Sending \{numFlightOrders} OrderFlightTransaction events");

        var customersCopy = new ArrayList<Customer>(customers);
        Collections.shuffle(customersCopy);
        var selectedCustomers = customersCopy.subList(0, numFlightOrders);

        var flightSeatsCopy = new ArrayList<FlightSeat>(flightSeats);
        Collections.shuffle(flightSeatsCopy);
        var selectedFlightSeats = flightSeatsCopy.subList(0, numFlightOrders);

        for(int i = 0; i < numFlightOrders; i++)
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
        System.out.println(STR."Unpaid bookings by id: \{unpaidBookings}");
    }
}
