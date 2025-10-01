package dk.ku.di.dms.vms.flightScheduler.flight.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;

@VmsTable(name="flight_seats")
@IdClass(FlightSeat.FlightSeatId.class)
public final class FlightSeat implements IEntity<FlightSeat.FlightSeatId> {

    public FlightSeat(int flight_id, String seat_number)
    {
        this.flight_id = flight_id;
        this.seat_number = seat_number;
        this.occupied = 0;
    }
    @SuppressWarnings("unused")
    public FlightSeat(){}

    // Id
    public static class FlightSeatId implements Serializable {
        public int flight_id;
        public String seat_number;

        @SuppressWarnings("unused")
        public FlightSeatId(){}

        public FlightSeatId(int flight_id, String seat_number) {
            this.flight_id = flight_id;
            this.seat_number = seat_number;
        }

        @Override
        public String toString() {
            return "[flight_id=" + flight_id + ", seat_number=" + seat_number + "]";
        }
    }

    @Id
    public int flight_id;
    @Id
    public String seat_number;
    @Column
    public int occupied;

    @Override
    public String toString() {
        return "{\n"
                + "  \"flight_id\":" + flight_id + ",\n"
                + "  \"seat_number\":\"" + seat_number + "\",\n"
                + "  \"occupied\":\"" + occupied + "\"\n"
                + "}";
    }
}
