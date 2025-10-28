package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.Abort.AbortTests;
import dk.ku.di.dms.vms.flightScheduler.test.Recovery.RecoveryTests;

import java.io.IOException;
import java.net.http.HttpClient;

public final class Main
{
    private static final HttpClient client = HttpClient.newHttpClient();

    public static void main(String[] args)
    {
//        Test.GetUnpaidBookings(client);
//        Test.Scenario1(client);
//        Test.FailedFlightOrder(client);
//        Test.FailedPayment(client);
//        Test.RecoveryTest(client);
//        AbortTests.CustomerCantAffordFlightSeat(client);
        try
        {
//            RecoveryTests.CustomerCrash(client);
            RecoveryTests.CoordinatorCrash(client);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //

}