package dk.ku.di.dms.vms.flightScheduler.test;

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
    public static List<Integer> GetUnpaidBookings(HttpClient client)
    {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8768/booking/unpaid"))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var payload = response.body();
            Integer[] bookingsArray = SERDES.deserialize(payload, Integer[].class);
            List<Integer> bookings = List.of(bookingsArray);
            return bookings;
        } catch (Exception e) {
            System.err.println("Failed to get unpaid bookings: ");
            return new ArrayList<Integer>();
        }
    }
}
