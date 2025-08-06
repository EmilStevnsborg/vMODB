package dk.ku.di.dms.vms.flightScheduler.test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

public final class Main
{
    private static final HttpClient client = HttpClient.newHttpClient();

    public static void main(String[] args)
    {
        // DATA INGESTION
        var numSeatsPerFlight = 100;
        var flightSeats = CreateFlightSeats("0", numSeatsPerFlight);

        var numCustomers = 20;
        var customers = CreateCustomers(numCustomers);


        // ORDER FLIGHTS
        var numFlightOrders = 2;

        var customersCopy = new ArrayList<Customer>(customers);
        Collections.shuffle(customersCopy);
        var selectedCustomers = customersCopy.subList(0, numFlightOrders);

        var flightSeatsCopy = new ArrayList<FlightSeat>(flightSeats);
        Collections.shuffle(flightSeatsCopy);
        var selectedFlightSeats = flightSeatsCopy.subList(0, numFlightOrders);

        OrderFlights(selectedCustomers, selectedFlightSeats);
    }

    //
    private static void OrderFlights(List<Customer> customers, List<FlightSeat> flightSeats)
    {
        if (customers.size() != flightSeats.size())
        {
            throw new IllegalArgumentException("Customers and flightSeats must have the same size.");
        }

        for (int i = 0; i < customers.size(); i++)
        {
            var customer = customers.get(i);
            var seat = flightSeats.get(i);

            var orderJson = STR."""
                    {
                      "customerId": "\{customer.customerId}",
                      "flightId": "\{seat.flightId}",
                      "seatNumber": "\{seat.seatNumber}"
                    }
                    """;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8766"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(orderJson))
                    .build();

            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                System.out.println("Order placed: " + orderJson);
            } catch (Exception e) {
                System.err.println("Failed to place order: " + orderJson);
            }
        }
    }

    // DATA GENERATION
    private static List<Customer> CreateCustomers(int numCustomers) {
        var customers = Collections.synchronizedList(new ArrayList<Customer>());
        for (int i = 0; i < numCustomers; i++) {
            var name = "user" + i;
            var customer = new Customer(String.valueOf(i), name);
            var customerJson = STR."""
                    {
                      "customerId": "\{customer.customerId}",
                      "name": "\{customer.name}"
                    }
                    """;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8769/customer"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(customerJson))
                    .build();

            try {
                client.send(request, HttpResponse.BodyHandlers.ofString());
                customers.add(customer);
            } catch (Exception e) {
                System.err.println("Failed to send: " + customerJson);
            }
        }
        return customers;
    }

    private static List<FlightSeat> CreateFlightSeats(String flightId, int numSeats) {
        var flightSeats = new ArrayList<FlightSeat>();
        var letters = List.of("A", "B", "C", "D");
        for (int i = 0; i < numSeats; i++) {

            var letter = letters.get(i % letters.size());
            int number = i / letters.size();
            var seatNumber = letter + number;
            var seat = new FlightSeat(flightId, seatNumber);
            var flightSeatJson = STR."""
                {
                  "flightId": "\{flightId}",
                  "seatNumber": "\{seatNumber}"
                }
                """;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8767/flight"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(flightSeatJson))
                    .build();

            try {
                client.send(request, HttpResponse.BodyHandlers.ofString());
                flightSeats.add(seat);
            } catch (Exception e) {
                System.err.println("Failed to send: " + flightSeatJson);
            }
        }
        return flightSeats;
    }

    // CLASSES
    public static class Customer {
        public final String customerId;
        public final String name;

        public Customer(String customerId, String name) {
            this.customerId = customerId;
            this.name = name;
        }
    }

    public static class FlightSeat {
        public final String flightId;
        public final String seatNumber;

        public FlightSeat(String flightId, String seatNumber) {
            this.flightId = flightId;
            this.seatNumber = seatNumber;
        }
    }

    // TESTS
}
