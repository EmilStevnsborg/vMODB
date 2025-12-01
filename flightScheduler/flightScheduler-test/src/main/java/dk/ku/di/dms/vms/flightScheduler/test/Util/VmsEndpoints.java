package dk.ku.di.dms.vms.flightScheduler.test.Util;


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
import java.util.Arrays;
import java.util.List;


public class VmsEndpoints
{
    protected static final IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();

    public static void Commit(HttpClient client)
    {
        HttpRequest cust_request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8769/customer/commit"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();

        HttpRequest fs_request = HttpRequest.newBuilder()
                .uri(URI.create(STR."http://localhost:8767/flight/commit"))
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();

//        HttpRequest b_request = HttpRequest.newBuilder()
//                .uri(URI.create("http://localhost:8768/booking/commit"))
//                .header("Content-Type", "application/json")
//                .POST(HttpRequest.BodyPublishers.ofString("{}"))
//                .build();
        try {
            client.send(cust_request, HttpResponse.BodyHandlers.ofString());
            client.send(fs_request, HttpResponse.BodyHandlers.ofString());
//            client.send(b_request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<Booking> GetBookings(HttpClient client)
    {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8768/booking"))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var payload = response.body();
            Booking[] bookingsArray = SERDES.deserialize(payload, Booking[].class);
            List<Booking> bookings = new ArrayList<>(Arrays.asList(bookingsArray));
            return bookings;
        } catch (Exception e) {
            System.err.println("Failed to get unpaid bookings: ");
            return new ArrayList<Booking>();
        }
    }

    public static List<Customer> GetCustomers(HttpClient client)
    {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8769/customer"))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var payload = response.body();
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


    public static int GetFlightSeatsCount(HttpClient client)
    {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(STR."http://localhost:8767/flight/count"))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var payload = response.body();
            return Integer.parseInt(payload);
        } catch (Exception e) {
            System.err.println("Failed to get flight seats");
            return -1;
        }
    }
}