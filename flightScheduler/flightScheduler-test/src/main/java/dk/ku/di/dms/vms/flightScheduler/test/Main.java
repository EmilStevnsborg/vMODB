package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.test.models.FlightSeat;

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
//        Test.GetUnpaidBookings(client);
        Test.Scenario1(client);
    }

    //

}