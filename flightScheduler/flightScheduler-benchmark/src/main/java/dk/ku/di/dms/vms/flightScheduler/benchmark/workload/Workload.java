package dk.ku.di.dms.vms.flightScheduler.benchmark.workload;

import dk.ku.di.dms.vms.flightScheduler.common.events.OrderFlight;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;


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
    public static Iterator<OrderFlight> createOrderFlightIterator(int numTransactions, int numberOfCustomers, int numberOfAborts)
    {
        var aborts = new HashSet<Integer>();
        for (int i = 1; i <= numberOfAborts; i++) {
            int tx = randomNumber(i*10000, (i+1)*10000);
            aborts.add(tx);
        }

        return new Iterator<>() {
            private int current = 1;

            @Override
            public boolean hasNext() {
                return current <= numTransactions;
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
}
