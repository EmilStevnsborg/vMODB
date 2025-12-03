package dk.ku.di.dms.vms.flightScheduler.benchmark.workload;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.flightScheduler.benchmark.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.common.events.OrderFlight;
import dk.ku.di.dms.vms.flightScheduler.common.events.PayBooking;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.ORDER_FLIGHT;
import static dk.ku.di.dms.vms.flightScheduler.common.Constants.PAY_BOOKING;


// FOR GENERATING ORDER_FLIGHTS
public class Workload
{
    private static final Random generate = new Random();
    public static int randomNumber(int min, int max) {
        int next = generate.nextInt();
        int div = next % ((max - min) + 1);
        if (div < 0) {
            div = div * -1;
        }
        return min + div;
    }
    public static Iterator<OrderFlight> createOrderFlightIterator(int numTransactions, int numberOfCustomers, int numberOfAborts, int initTx)
    {
        var aborts = new HashSet<Integer>();
        if (numberOfAborts > 0) {
            // start
            var start = initTx + numTransactions/5;
            var increment = (numTransactions-start)/numberOfAborts;

            for (int i = 0; i < numberOfAborts; i++) {
                var tx = start + i*increment;
                System.out.println(STR."Abort orderFlight tx=\{tx}");
                aborts.add(tx);
            }
        }

        return new Iterator<>() {
            private int current = initTx;

            @Override
            public boolean hasNext() {
                return current <= initTx+numTransactions;
            }
            @Override
            public OrderFlight next() {
                if (!hasNext()) throw new NoSuchElementException();
                int txIdx = current++;
                boolean abortTx = aborts.contains(txIdx);

                if (abortTx) {
                    System.out.println(STR."abort tx: \{txIdx} by setting cId to \{numberOfCustomers}");
                    return new OrderFlight(numberOfCustomers, txIdx, txIdx); // or custom abort version
                } else {
                    return new OrderFlight(txIdx, txIdx, txIdx);
                }
            }
        };
    }

    public static Iterator<PayBooking> createPayBookingIterator(int numTransactions, int initTx)
    {
        return new Iterator<>() {
            private int current = initTx;

            @Override
            public boolean hasNext() {
                return current <= initTx+numTransactions;
            }
            @Override
            public PayBooking next() {
                if (!hasNext()) throw new NoSuchElementException();
                int txIdx = current++;
                return new PayBooking(txIdx, "VISA"); // or custom abort version
            }
        };
    }

    // WORKLOAD SUBMISSION


    // mixed workload: orderFlight and payBooking
    public static WorkloadStats submitMixedWorkload(Iterator<OrderFlight> orderFlightsIterator,
                                                    Iterator<PayBooking> payBookingsIterator,
                                                    final Coordinator coordinator, int delay, int batchSize)
    {
        var payBookingFunction = payBookingBuilder(coordinator);
        var orderFlightFunction = orderFlightBuilder(coordinator);

        Thread thread1 = new Thread(() -> {
            var i = 0;
            while (payBookingsIterator.hasNext()) {
                try {
                    if (i >= batchSize) {
                        Util.Sleep(delay);
                        i=0;
                        // System.out.println("queuing payBookings stopped sleeping");
                    }
                    payBookingFunction.accept(payBookingsIterator.next());
                    i++;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Thread thread2 = new Thread(() -> {
            var i = 0;
            while (orderFlightsIterator.hasNext()) {
                try {
                    if (i >= batchSize) {
                        Util.Sleep(delay);
                        i=0;
                        // System.out.println("queuing orderFlights stopped sleeping");
                    }
                    orderFlightFunction.accept(orderFlightsIterator.next());
                    i++;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        thread1.start();
        thread2.start();
        return new WorkloadStats(System.currentTimeMillis());
    }


    // pay booking
    private static Consumer<PayBooking> payBookingBuilder(final Coordinator coordinator) {
        return payBooking -> {
            var eventPayload = new TransactionInput.Event(PAY_BOOKING, payBooking.toString());
            var txInput = new TransactionInput(PAY_BOOKING, eventPayload);
            coordinator.queueTransactionInput(txInput);
        };
    }
    public static WorkloadStats submitPayBookings(Iterator<PayBooking> payBookingsIterator, final Coordinator coordinator)
    {
        var payBookingFunction = payBookingBuilder(coordinator);
        Thread thread = new Thread(() -> {
            while (payBookingsIterator.hasNext()) {
                try {
                    payBookingFunction.accept(payBookingsIterator.next());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();
        return new WorkloadStats(System.currentTimeMillis());
    }


    // order flight
    private static Consumer<OrderFlight> orderFlightBuilder(final Coordinator coordinator) {
        return orderFlight -> {
            var eventPayload = new TransactionInput.Event(ORDER_FLIGHT, orderFlight.toString());
            var txInput = new TransactionInput(ORDER_FLIGHT, eventPayload);
            coordinator.queueTransactionInput(txInput);
        };
    }

    public record WorkloadStats(long initTs){}
    public static WorkloadStats submitOrderFlights(Iterator<OrderFlight> orderFlightsIterator, final Coordinator coordinator)
    {
        var orderFlightFunction = orderFlightBuilder(coordinator);
        Thread thread = new Thread(() -> {
            while (orderFlightsIterator.hasNext()) {
                try {
                    orderFlightFunction.accept(orderFlightsIterator.next());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();
        return new WorkloadStats(System.currentTimeMillis());
    }
}
