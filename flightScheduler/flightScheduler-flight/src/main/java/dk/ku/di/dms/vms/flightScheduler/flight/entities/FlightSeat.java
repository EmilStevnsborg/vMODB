package dk.ku.di.dms.vms.flightScheduler.flight.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;

@VmsTable(name="flight_seats")
public final class FlightSeat implements IEntity<FlightSeat.FlightSeatId> {

    public FlightSeat(int flightId, String seatNumber)
    {
        this.flightId = flightId;
        this.seatNumber = seatNumber;
        this.occupied = 0;
    }
    @SuppressWarnings("unused")
    public FlightSeat(){}

    // Id
    public static class FlightSeatId implements Serializable {
        public int flightId;
        public String seatNumber;

        @SuppressWarnings("unused")
        public FlightSeatId(){}

        public FlightSeatId(int flightId, String seatNumber) {
            this.flightId = flightId;
            this.seatNumber = seatNumber;
        }
    }

    @Id
    public int flightId;
    @Id
    public String seatNumber;
    @Column
    public int occupied;

    @Override
    public String toString() {
        return "{\n"
                + "  \"flightId\":" + flightId + ",\n"
                + "  \"seatNumber\":\"" + seatNumber + ",\"\n"
                + "  \"occupied\":\"" + occupied + "\"\n"
                + "}";
    }
}
