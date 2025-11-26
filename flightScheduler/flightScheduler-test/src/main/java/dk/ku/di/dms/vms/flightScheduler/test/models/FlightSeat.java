package dk.ku.di.dms.vms.flightScheduler.test.models;

public class FlightSeat
{
    public int flight_id;
    public int seat_number;
    public int occupied;

    public FlightSeat(int flight_id, int seat_number) {
        this.flight_id = flight_id;
        this.seat_number = seat_number;
        occupied = 0;
    }
    public FlightSeat(){}
    @Override
    public String toString()
    {
        return "{"
                + "  \"flight_id\":\"" + flight_id + "\",\n"
                + "  \"seat_number\":\"" + seat_number + "\",\n"
                + "  \"occupied\":\"" + occupied + "\"\n"
                + "\n}";
    }
}
