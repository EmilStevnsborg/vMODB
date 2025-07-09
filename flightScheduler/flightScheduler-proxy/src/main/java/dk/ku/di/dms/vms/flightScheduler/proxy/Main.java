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

    public static void main(String[] ignoredArgs) {
        Properties properties = ConfigUtils.loadProperties();
        System.out.println(properties);
        loadCoordinator(properties);
    }

    private static void loadCoordinator(Properties properties)
    {
        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG orderFlightDag = TransactionBootstrap.name(ORDER_FLIGHT)
                .input("a", "flight", ORDER_FLIGHT)
                .internal("b", "booking", BOOK_SEAT, "a")
                .terminal("c", "customer", "b")
                .build();

        transactionMap.put(orderFlightDag.name, orderFlightDag);

        Map<String, IdentifiableNode> starterVMSs = new HashMap<>();

        String flightHost = properties.getProperty("flight_host");
        String bookingHost = properties.getProperty("booking_host");
        String customerHost = properties.getProperty("customer_host");

        IdentifiableNode flightAddress = new IdentifiableNode("flight", flightHost, FLIGHT_VMS_PORT);
        IdentifiableNode bookingAddress = new IdentifiableNode("booking", bookingHost, BOOKING_VMS_PORT);
        IdentifiableNode customerAddress = new IdentifiableNode("customer", customerHost, CUSTOMER_VMS_PORT);

        starterVMSs.putIfAbsent(flightAddress.identifier, flightAddress);
        starterVMSs.putIfAbsent(bookingAddress.identifier, bookingAddress);
        starterVMSs.putIfAbsent(customerAddress.identifier, customerAddress);

        Coordinator coordinator = Coordinator.build(properties, starterVMSs, transactionMap, ProxyHttpServerAsyncJdk::new);

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();
    }
}
