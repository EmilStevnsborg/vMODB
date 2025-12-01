package dk.ku.di.dms.vms.flightScheduler.test.BasicTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsEndpoints;
import dk.ku.di.dms.vms.flightScheduler.test.models.Booking;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.http.HttpClient;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

public class BookingInjection
{
    public static boolean Run(HttpClient client) throws IOException {

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

        // wait for components to come online
        System.console().readLine();
        System.out.println("TEST: injecting data");

        // injecting fs (occupied), customers (with fs seat_number), booking (with f_id and c_id)

        var customers = DataGenerator.CreateCustomers(100, 0);
        var flightSeats = DataGenerator.CreateFlightSeats(100, 0, 0);

        for (int i = 0; i < 100; i++) {
            var customer = customers.get(i);
            var flightSeat = flightSeats.get(i);

            if (i < 50) {
                flightSeat.occupied = 1;
                customer.seat_number = flightSeat.seat_number;
                var booking = new Booking(-1, customer.customer_id, flightSeat.flight_id, flightSeat.seat_number, new Date().toString(), 20);
                DataGenerator.SendBooking(client, booking);
            }
            DataGenerator.SendCustomer(client, customer);
            DataGenerator.SendFlightSeat(client, flightSeat);
        }

        System.console().readLine();
        System.out.println("TEST: committing");
        VmsEndpoints.Commit(client);


        System.console().readLine();
        System.out.println("TEST: get data");

        var bookingsRetrieved = VmsEndpoints.GetBookings(client);
        var customersRetrieved = VmsEndpoints.GetCustomers(client);
        var flightSeatsRetrieved = VmsEndpoints.GetFlightSeats(client, 0);

        var customerThatDontHaveSeats = customersRetrieved.stream().filter(c -> c.seat_number == -1).count();
        var flightSeatsOccupied = flightSeatsRetrieved.stream().filter(fs -> fs.occupied == 1).count();

        System.out.println(STR."customersRetrieved=\{customersRetrieved.size()} " +
                           STR."flightSeatsRetrieved=\{flightSeatsRetrieved.size()}, " +
                           STR."bookingsRetrieved=\{bookingsRetrieved.size()}, " +
                           STR."customerThatDontHaveSeats=\{customerThatDontHaveSeats}, " +
                           STR."flightSeatsOccupied=\{flightSeatsOccupied}");

        var success = true;
        return success;
    }
}
