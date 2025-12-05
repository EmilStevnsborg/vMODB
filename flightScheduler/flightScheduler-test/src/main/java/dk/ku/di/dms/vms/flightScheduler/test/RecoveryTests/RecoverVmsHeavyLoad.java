package dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Transactions;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.models.Booking;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Date;

public class RecoverVmsHeavyLoad {

    public static boolean Run(HttpClient client) throws IOException
    {
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 1, 1, 1,
                    1000, 2000);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // wait for components to come online
        System.console().readLine();
        System.out.println("TEST: injecting data");

        var customers = DataGenerator.GenerateCustomers(client , 20000, 0);
        var flightSeats = DataGenerator.GenerateFlightSeats(client , 0, 20000, 0);

        System.out.println(STR."TEST: injecting bookings");
        var bookings = new ArrayList<Booking>();
        for (int i = customers.size()/2; i < customers.size(); i++) {
            var j = i-customers.size()/2;

            var customer = customers.get(i);
            var flightSeat = flightSeats.get(i);
            var booking = new Booking(j, customer.customer_id, flightSeat.flight_id, flightSeat.seat_number, new Date().toString(), 20);
            bookings.add(booking);
            DataGenerator.SendBooking(client, booking);
        }

        System.out.println(STR."TEST: done injecting");

        // batch 1-3, should seal and commit at least two batches
        System.console().readLine();
        System.out.println(STR."TEST: sending transactions");
        for (var i = 0; i < customers.size()/2; i++) {
            if (i == 2500) {
                ComponentProcess.Kill("payment");
            }
            if (i == 5000) {
                ComponentProcess.StartVms("payment", true, 1);
            }
            if (i % 1000 == 0) {
                Util.Sleep(1000);
            }

            Transactions.OrderFlight(client, customers.get(i), flightSeats.get(i));
            Transactions.PayBooking(client, i, "VISA");
        }

        System.console().readLine();

        var success = true;
        return success;
    }
}
