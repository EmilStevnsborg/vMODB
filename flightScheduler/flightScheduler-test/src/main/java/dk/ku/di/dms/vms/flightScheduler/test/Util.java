//package dk.ku.di.dms.vms.flightScheduler.test;
//
//import java.net.URI;
//import java.net.http.HttpClient;
//import java.net.http.HttpRequest;
//import java.net.http.HttpResponse;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//
//public class Util
//{
//    private static final HttpClient client = HttpClient.newHttpClient();
//
//    public static void SendFlightSeat(Main.FlightSeat flightSeat)
//    {
//        HttpRequest request = HttpRequest.newBuilder()
//                .uri(URI.create("http://localhost:8767/flight"))
//                .header("Content-Type", "application/json")
//                .POST(HttpRequest.BodyPublishers.ofString(flightSeat.toString()))
//                .build();
//
//        try {
//            client.send(request, HttpResponse.BodyHandlers.ofString());
//        } catch (Exception e) {
//            System.err.println("Failed to send: " + flightSeat);
//        }
//    }
//
//    public static void SendCustomer(Main.Customer customer)
//    {
//        HttpRequest request = HttpRequest.newBuilder()
//                .uri(URI.create("http://localhost:8769/customer"))
//                .header("Content-Type", "application/json")
//                .POST(HttpRequest.BodyPublishers.ofString(customer.toString()))
//                .build();
//
//        try {
//            client.send(request, HttpResponse.BodyHandlers.ofString());
//        } catch (Exception e) {
//            System.err.println("Failed to send: " + customer);
//        }
//    }
//
//    public static List<Main.FlightSeat> CreateFlightSeats(int flightId, int numSeats) {
//        var flightSeats = new ArrayList<Main.FlightSeat>();
//        var letters = List.of("A", "B", "C", "D");
//        for (int i = 0; i < numSeats; i++) {
//            var letter = letters.get(i % letters.size());
//            int number = i / letters.size();
//            var seatNumber = letter + number;
//            var seat = new Main.FlightSeat(flightId, seatNumber);
//            flightSeats.add(seat);
//        }
//        return flightSeats;
//    }
//
//    public static List<Main.Customer> CreateCustomers(int numCustomers)
//    {
//        var customers = Collections.synchronizedList(new ArrayList<Main.Customer>());
//        for (int i = 0; i < numCustomers; i++) {
//            var name = "user" + i;
//            var customer = new Main.Customer(i, name);
//            customers.add(customer);
//        }
//        return customers;
//    }
//}
