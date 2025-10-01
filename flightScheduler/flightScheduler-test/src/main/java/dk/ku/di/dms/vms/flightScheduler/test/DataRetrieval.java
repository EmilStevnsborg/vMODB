package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.models.Booking;
import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.test.models.FlightSeat;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

public class DataRetrieval
{
    protected static final IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();
    public static List<Booking> GetUnpaidBookings(HttpClient client)
    {
        System.out.println("Get unpaid bookings");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8768/booking/unpaid"))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var payload = response.body();
//            System.out.println(STR."Unpaid booking payload=\{payload}");
            Booking[] bookingsArray = SERDES.deserialize(payload, Booking[].class);
            List<Booking> bookings = List.of(bookingsArray);
            return bookings;
        } catch (Exception e) {
            System.err.println("Failed to get unpaid bookings: ");
            return new ArrayList<Booking>();
        }
    }
    public static List<Booking> GetBookings(HttpClient client)
    {
        System.out.println("Get bookings");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8768/booking"))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var payload = response.body();
//            System.out.println(STR."Customers payload=\{payload}");
            Booking[] bookingsArray = SERDES.deserialize(payload, Booking[].class);
            List<Booking> bookings = List.of(bookingsArray);
            return bookings;
        } catch (Exception e) {
            System.err.println("Failed to get unpaid bookings: ");
            return new ArrayList<Booking>();
        }
    }

    public static List<Customer> GetCustomers(HttpClient client)
    {
        System.out.println("Get customers");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8769/customer"))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var payload = response.body();
//            System.out.println(STR."Customers payload=\{payload}");
            Customer[] customersArray = SERDES.deserialize(payload, Customer[].class);
            List<Customer> customers = List.of(customersArray);
            return customers;
        } catch (Exception e) {
            System.err.println("Failed to get customers");
            return new ArrayList<Customer>();
        }
    }

    public static List<FlightSeat> GetFlightSeats(HttpClient client, int flightId)
    {
        System.out.println("Get flight seats");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(STR."http://localhost:8767/flight/\{flightId}"))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var payload = response.body();
            FlightSeat[] flightSeatsArray = SERDES.deserialize(payload, FlightSeat[].class);
            List<FlightSeat> flightSeats = List.of(flightSeatsArray);
            return flightSeats;
        } catch (Exception e) {
            System.err.println("Failed to get flight seats");
            return new ArrayList<FlightSeat>();
        }
    }
    public static Customer GetCustomer(HttpClient client, int id)
    {
        System.out.println(STR."Get customer cId=\{id}");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(STR."http://localhost:8769/customer\{id}"))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var payload = response.body();
//            System.out.println(STR."Customers payload=\{payload}");
            Customer customer = SERDES.deserialize(payload, Customer.class);
            return customer;
        } catch (Exception e) {
            System.err.println("Failed to get customer");
            return null;
        }
    }
}
