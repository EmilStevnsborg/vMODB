package dk.ku.di.dms.vms.flightScheduler.benchmark.models;

public class Booking
{
    public int booking_id;
    public int customer_id;
    public int flight_id;
    public int seat_number;
    public String timestamp;
    public int paid;
    public int price;
    public Booking(int booking_id, int customer_id, int flight_id, int seat_number, String timestamp, int price)
    {
        this.booking_id = booking_id;
        this.customer_id = customer_id;
        this.flight_id = flight_id;
        this.seat_number = seat_number;
        this.timestamp = timestamp;
        this.paid = 0;
        this.price = price;

    }

    public Booking(){}

    @Override
    public String toString() {
        return "{\n"
                + "  \"booking_id\":" + booking_id + ",\n"
                + "  \"customer_id\":" + customer_id + ",\n"
                + "  \"flight_id\":" + flight_id + ",\n"
                + "  \"seat_number\":\"" + seat_number + "\",\n"
                + "  \"timestamp\":\"" + timestamp + "\",\n"
                + "  \"paid\":\"" + paid + "\",\n"
                + "  \"price\":\"" + price + "\"\n"
                + "}";
    }
}

