package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class BookingCancelled
{
    public int booking_id;
    public int requires_reimbursement;
    public int customer_id;
    public int flight_id;
    public int seat_number;
    public int reimbursement;

    public BookingCancelled(){}
    public BookingCancelled(int booking_id, int requires_reimbursement, int customer_id,
                            int flight_id, int seat_number, int reimbursement)
    {
        this.booking_id = booking_id;
        this.requires_reimbursement = requires_reimbursement;
        this.customer_id = customer_id;
        this.flight_id = flight_id;
        this.seat_number = seat_number;
        this.reimbursement = reimbursement;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "  \"booking_id\":" + booking_id + ",\n"
                + "  \"requires_reimbursement\":" + requires_reimbursement + ",\n"
                + "  \"customer_id\":" + customer_id + ",\n"
                + "  \"flight_id\":" + flight_id + ",\n"
                + "  \"seatNumber\":" + seat_number + ",\n"
                + "  \"reimbursement\":" + reimbursement + "\n"
                + "\n}";
    }
}
