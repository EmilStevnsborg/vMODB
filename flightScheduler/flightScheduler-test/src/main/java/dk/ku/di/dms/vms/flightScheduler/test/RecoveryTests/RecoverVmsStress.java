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

public class RecoverVmsStress
{
    public static boolean Run(HttpClient client) throws IOException {
        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 2, 2, 2,
                    700, 10);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // wait for components to come online
        System.console().readLine();
        System.out.println("TEST: injecting data");

        var customers = DataGenerator.CreateCustomers(300, 0);
        var flightSeats = DataGenerator.CreateFlightSeats(300, 0, 0);
        var bookings = new ArrayList<Booking>();

        for (int i = 0; i < customers.size(); i++) {
            var customer = customers.get(i);
            var flightSeat = flightSeats.get(i);

            if (i < customers.size()/2) {
                flightSeat.occupied = 1;
                customer.seat_number = flightSeat.seat_number;
                var booking = new Booking(i, customer.customer_id, flightSeat.flight_id, flightSeat.seat_number, new Date().toString(), 20);
                bookings.add(booking);
                DataGenerator.SendBooking(client, booking);
            }
            DataGenerator.SendCustomer(client, customer);
            DataGenerator.SendFlightSeat(client, flightSeat);
        }
        System.out.println(STR."TEST: done injecting");

        // batch 1-3, should seal and commit at least two batches
        System.console().readLine();
        System.out.println(STR."TEST: sending transactions");
        for (var i = 0; i < customers.size()/2; i++) {
            if (i == 40) {
                ComponentProcess.Kill("flight");
            }
            if (i % 10 == 0) {
                Util.Sleep(200);
            }
            Transactions.OrderFlight(client, customers.get(i+customers.size()/2), flightSeats.get(i+customers.size()/2));
            Transactions.PayBooking(client, i, "VISA");
        }

        System.console().readLine();

        var success = true;
        return success;
    }
}
