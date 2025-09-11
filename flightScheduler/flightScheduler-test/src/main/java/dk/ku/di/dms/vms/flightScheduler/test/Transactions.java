package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.test.models.FlightSeat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class Transactions
{
    public static void OrderFlight(HttpClient client, Customer customer, FlightSeat flightSeat)
    {
        var orderJson = STR."""
                    {
                      "customer_id": "\{customer.customer_id}",
                      "flight_id": "\{flightSeat.flight_id}",
                      "seat_number": "\{flightSeat.seat_number}"
                    }
                    """;

        System.out.println("Sending: \n" + orderJson);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8766/orderFlight"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(orderJson))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.err.println("Failed to place order");
        }
    }

    // PayBooking
    public static void PayBooking(HttpClient client, int booking_id, String payment_method)
    {
        var payBookingJson = STR."""
                    {
                      "booking_id": "\{booking_id}",
                      "payment_method": "\{payment_method}"
                    }
                    """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8766/payBooking"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payBookingJson))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.err.println("Failed to pay booking");
        }
    }

    // CancelBooking
    public static void CancelBooking(HttpClient client, int booking_id)
    {
        var cancelBookingJson = STR."""
                    {
                      "booking_id": "\{booking_id}"
                    }
                    """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8766/cancelBooking"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(cancelBookingJson))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.err.println("Failed to cancel booking");
        }
    }

    // ReimburseBooking
    public static void ReimburseBooking(HttpClient client, int booking_id)
    {
        var reimburseBookingJson = STR."""
                    {
                      "booking_id": "\{booking_id}"
                    }
                    """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8766/reimburseBooking"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(reimburseBookingJson))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.err.println("Failed to reimburse booking");
        }
    }
}
