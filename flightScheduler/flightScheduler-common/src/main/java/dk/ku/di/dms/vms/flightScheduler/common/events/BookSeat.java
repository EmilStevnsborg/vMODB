package dk.ku.di.dms.vms.flightScheduler.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.Date;
import java.util.stream.Collectors;

@Event
public class BookSeat
{
    public String timestamp;
    public OrderFlight orderFlight;

    public BookSeat(){}
    public BookSeat(String timestamp, OrderFlight orderFlight)
    {
        this.timestamp = timestamp;
        this.orderFlight = orderFlight;
    }

    @Override
    public String toString()
    {
        return "{\n"
                + "  \"timestamp\":" + timestamp + ",\n"
                + "  \"orderFlight\":"
                + indent(orderFlight.toString(), "    ")
                + "\n"
                + "}";
    }
    private String indent(String input, String prefix) {
        return input.lines().map(line -> prefix + line).collect(Collectors.joining("\n"));
    }
}
