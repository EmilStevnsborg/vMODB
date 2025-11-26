package dk.ku.di.dms.vms.flightScheduler.flight.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;

@VmsTable(name="flight_seats")
@IdClass(FlightSeat.FlightSeatId.class)
public final class FlightSeat implements IEntity<Integer> {

    // using two ids causes some problems
    public static class FlightSeatId implements Serializable {
        public int flight_id;
        public int seat_number;

        @SuppressWarnings("unused")
        public FlightSeatId(){}

        public FlightSeatId(int flight_id, int seat_number) {
            this.flight_id = flight_id;
            this.seat_number = seat_number;
        }
    }
    public FlightSeat(int flight_id, int seat_number)
    {
        this.flight_id = flight_id;
        this.seat_number = seat_number;
        this.occupied = 0;
    }
    @SuppressWarnings("unused")
    public FlightSeat(){}

    // Can't have two ids?
    @Column
    public int flight_id;
    @Id
    public int seat_number;
    @Column
    public int occupied;

    public void flightSeatHasBeenBooked()
    {
        this.occupied = 1;
    }
    @Override
    public String toString() {
        return "{\n"
                + "  \"flight_id\":" + flight_id + ",\n"
                + "  \"seat_number\":" + seat_number + ",\n"
                + "  \"occupied\":\"" + occupied + "\"\n"
                + "}";
    }
}
