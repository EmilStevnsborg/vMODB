package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.models.Customer;
import dk.ku.di.dms.vms.flightScheduler.test.models.FlightSeat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataCreation
{
    public static List<Customer> CreateCustomers(int numCustomers)
    {
        var customers = Collections.synchronizedList(new ArrayList<Customer>());
        for (int i = 0; i < numCustomers; i++) {
            var name = "user" + i;
            var customer = new Customer(i, 100, name);
            customers.add(customer);
        }
        return customers;
    }
    public static List<FlightSeat> CreateFlightSeats(int numSeats, int flight_id)
    {
        var flightSeats = new ArrayList<FlightSeat>();
        var letters = List.of("A", "B", "C", "D");
        for (int i = 0; i < numSeats; i++) {

            var letter = letters.get(i % letters.size());
            int number = i / letters.size();
            var seat_number = letter + number;
            var seat = new FlightSeat(flight_id, seat_number);
            flightSeats.add(seat);
        }
        return flightSeats;
    }
}
