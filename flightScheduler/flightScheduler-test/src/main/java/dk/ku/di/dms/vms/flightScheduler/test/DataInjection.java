package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.test.models.FlightSeat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class DataInjection
{
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
