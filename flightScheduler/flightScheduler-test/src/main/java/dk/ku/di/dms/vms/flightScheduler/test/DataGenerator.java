package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.test.models.FlightSeat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataGenerator
{
    public static List<Customer> CreateCustomers(int numCustomers, int statingId)
    {
        var customers = new ArrayList<Customer>();
        for (int i = statingId; i < numCustomers+statingId; i++) {
            var customer = new Customer(i, 100, STR."user_\{i}");
            customers.add(customer);
        }
        return customers;
    }
    public static List<Customer> GenerateCustomers(HttpClient client, int numCustomers)
    {
        var customers = new ArrayList<Customer>();
        for (int i = 0; i < numCustomers; i++) {
            var customer = new Customer(i, 100, STR."user_\{i}");
            customers.add(customer);
            SendCustomer(client, customer);
        }
        return customers;
    }
    public static List<FlightSeat> GenerateFlightSeats(HttpClient client, int flight_id, int numSeats)
    {
        var flightSeats = new ArrayList<FlightSeat>();
        var letters = List.of("A", "B", "C", "D");
        for (int i = 0; i < numSeats; i++) {
            var letter = letters.get(i % letters.size());
            int number = i / letters.size();
            var seat_number = letter + number;
            var seat = new FlightSeat(flight_id, seat_number);
            flightSeats.add(seat);
            SendFlightSeat(client, seat);
        }
        return flightSeats;
    }

    public static void SendCustomer(HttpClient client, Customer customer)
    {
        var customerJson = customer.toString();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8769/customer"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(customerJson))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.err.println("Failed to send: " + customerJson);
        }
    }

    public static void SendFlightSeat(HttpClient client, FlightSeat flightSeat)
    {
        var flightSeatJson = flightSeat.toString();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8767/flight"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(flightSeatJson))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.err.println("Failed to send: " + flightSeatJson);
        }
    }
}
