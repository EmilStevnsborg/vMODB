package dk.ku.di.dms.vms.flightScheduler.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;

public final class Main
{

    public static void main(String[] args)
    {
        Properties properties = ConfigUtils.loadProperties();

        if (args != null && args.length > 0) {

            for (var arg : args)
            {
                var argSplit = arg.split("=");
                if (argSplit.length != 2) {
                    System.out.println(STR."invalid arg");
                    continue;
                }

                var argName = argSplit[0];
                var argValue = argSplit[1];
                properties.setProperty(argName, argValue);
            }

            loadCoordinator(properties);
        } else {
            loadCoordinator(properties);
        }
    }

    public static Coordinator loadCoordinator(Properties properties)
    {
        Map<String, TransactionDAG> transactionMap = new HashMap<>();

        // OrderFlight
        TransactionDAG orderFlightDag = TransactionBootstrap.name(ORDER_FLIGHT)
                .input("a", "flight", ORDER_FLIGHT)
                .internal("b", "booking", BOOK_SEAT, "a")
                .terminal("c", "customer", "b")
                .build();
        transactionMap.put(orderFlightDag.name, orderFlightDag);

        // PayBooking
        TransactionDAG payBookingDag = TransactionBootstrap.name(PAY_BOOKING)
                .input("a", "payment", PAY_BOOKING)
                .internal("b", "booking", PAYMENT_SUCCEEDED, "a")
                .terminal("c", "customer",  "b")
                .build();
        transactionMap.put(payBookingDag.name, payBookingDag);

        // CancelBooking
        TransactionDAG cancelBookingDag = TransactionBootstrap.name(CANCEL_BOOKING)
                .input("a", "booking", CANCEL_BOOKING)
                .terminal("b", "payment",  "a")
                .terminal("b", "flight",  "a")
                .build();
        transactionMap.put(cancelBookingDag.name, cancelBookingDag);

        // ReimburseBooking
        TransactionDAG reimburseBookingDag = TransactionBootstrap.name(REIMBURSE_BOOKING)
                .input("a", "payment", REIMBURSE_BOOKING)
                .terminal("b", "customer",  "a")
                .build();
        transactionMap.put(reimburseBookingDag.name, reimburseBookingDag);


        Map<String, IdentifiableNode> starterVMSs = new HashMap<>();

        String flightHost = properties.getProperty("flight_host");
        String bookingHost = properties.getProperty("booking_host");
        String customerHost = properties.getProperty("customer_host");
        String paymentHost = properties.getProperty("payment_host");

        IdentifiableNode flightAddress = new IdentifiableNode("flight", flightHost, FLIGHT_VMS_PORT);
        IdentifiableNode bookingAddress = new IdentifiableNode("booking", bookingHost, BOOKING_VMS_PORT);
        IdentifiableNode customerAddress = new IdentifiableNode("customer", customerHost, CUSTOMER_VMS_PORT);
        IdentifiableNode paymentAddress = new IdentifiableNode("payment", paymentHost, PAYMENT_VMS_PORT);

        starterVMSs.putIfAbsent(flightAddress.identifier, flightAddress);
        starterVMSs.putIfAbsent(bookingAddress.identifier, bookingAddress);
        starterVMSs.putIfAbsent(customerAddress.identifier, customerAddress);
        starterVMSs.putIfAbsent(paymentAddress.identifier, paymentAddress);

        Coordinator coordinator = Coordinator.build(properties, starterVMSs,
                transactionMap, ProxyHttpServerAsyncJdk::new);

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        return coordinator;
    }
}
