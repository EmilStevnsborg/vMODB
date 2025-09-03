package dk.ku.di.dms.vms.flightScheduler.test.models;

public class FlightSeat
{
    public final int flight_id;
    public final String seat_number;

    public FlightSeat(int flight_id, String seat_number) {
        this.flight_id = flight_id;
        this.seat_number = seat_number;
    }
    @Override
    public String toString()
    {
        return "{"
                + "  \"flight_id\":\"" + flight_id + "\","
                + "  \"seat_number\":\"" + seat_number + "\""
                + "\n}";
    }
}
