package dk.ku.di.dms.vms.tpcc.benchmark.workload;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.benchmark.Util.Util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.nuRand;
import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.randomNumber;
import static dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants.*;

public class Workload
{

    public static Iterator<NewOrderWareIn> createNewOrderIterator(int numTransactions)
    {
        var aborts = new HashSet<Integer>();

        return new Iterator<>() {
            private int current = 1;

            @Override
            public boolean hasNext() {
                return current <= numTransactions;
            }
            @Override
            public NewOrderWareIn next() {
                if (!hasNext()) throw new NoSuchElementException();
                int txIdx = current++;

                var newOrder = generateNewOrder();
                return newOrder;
            }
        };
    }

    private static Consumer<NewOrderWareIn> newOrderInputBuilder(final Coordinator coordinator) {
        return newOrderWareIn -> {
            TransactionInput.Event eventPayload = new TransactionInput.Event("new-order-ware-in", newOrderWareIn.toString());
            TransactionInput txInput = new TransactionInput("new_order", eventPayload);
            coordinator.queueTransactionInput(txInput);
        };
    }

    public static Thread submitNewOrders(Iterator<NewOrderWareIn> newOrdersIterator,
                                           final Coordinator coordinator, int delay, int batchSize)
    {
        var payBookingFunction = newOrderInputBuilder(coordinator);
        Thread thread = new Thread(() -> {
            var i = 0;
            while (!Thread.currentThread().isInterrupted() && newOrdersIterator.hasNext()) {
                try {
                    if (i >= batchSize) {
                        Util.Sleep(delay);
                        i=0;
                        // System.out.println("queuing payBookings stopped sleeping");
                    }
                    payBookingFunction.accept(newOrdersIterator.next());
                    i++;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();
        return thread;
    }

    public static NewOrderWareIn generateNewOrder()
    {
        int w_id = 1;
        int d_id;
        int c_id;
        int ol_cnt;
        int all_local = 1;
        int not_found = NUM_ITEMS + 1;

        d_id = randomNumber(1, NUM_DIST_PER_WARE);
        c_id = nuRand(1023, 1, NUM_CUST_PER_DIST);

        ol_cnt = randomNumber(MIN_NUM_ITEMS_PER_ORDER, MAX_NUM_ITEMS_PER_ORDER);

        int[] itemIds = new int[ol_cnt];
        int[] supWares = new int[ol_cnt];
        int[] qty = new int[ol_cnt];

        for (int i = 0; i < ol_cnt; i++)
        {
            int item_ = nuRand(8191, 1, NUM_ITEMS);

            // avoid duplicate items
            while(foundItem(itemIds, i, item_)){
                item_ = nuRand(8191, 1, NUM_ITEMS);
            }
            itemIds[i] = item_;

            // add abort here

            supWares[i] = w_id;

            qty[i] = randomNumber(1, 10);
        }

        return new NewOrderWareIn(w_id, d_id, c_id, itemIds, supWares, qty, all_local == 1);
    }

    private static boolean foundItem(int[] itemIds, int length, int value){
        if(length == 0) return false;
        for(int i = 0; i < length; i++){
            if(itemIds[i] == value) return true;
        }
        return false;
    }
}
