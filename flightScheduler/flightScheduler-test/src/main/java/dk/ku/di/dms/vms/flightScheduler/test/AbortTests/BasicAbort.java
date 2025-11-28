package dk.ku.di.dms.vms.flightScheduler.test.AbortTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class BasicAbort
{
    public static boolean Run(HttpClient client) {
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");

        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 1, 1, 1,
                    Integer.MAX_VALUE, 10);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }


        System.console().readLine();
        System.out.println("TEST: Injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 19);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 30);

        VmsEndpoints.Commit(client);

        System.console().readLine();
        System.out.println(STR."TEST: sending order_flights with TIDs of [1-31)");
        for (var i = 0; i < flightSeats.size(); i++)
        {

            var orderJson = STR."""
                    {
                      "customer_id": "\{i}",
                      "flight_id": "\{0}",
                      "seat_number": "\{i}"
                    }
                    """;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8766/orderFlight"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(orderJson))
                    .build();

            try {
                client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
            }
        }

        System.console().readLine();

        var success = true;
        return success;
    }
}
