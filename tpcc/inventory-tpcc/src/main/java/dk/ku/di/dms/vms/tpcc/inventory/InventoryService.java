package dk.ku.di.dms.vms.tpcc.inventory;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderInvOut;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareOut;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("inventory")
public final class InventoryService {

    private final IItemRepository itemRepository;
    private final IStockRepository stockRepository;

    public InventoryService(IItemRepository itemRepository, IStockRepository stockRepository) {
        this.itemRepository = itemRepository;
        this.stockRepository = stockRepository;
    }

    @Inbound(values = "new-order-ware-out")
    @Outbound("new-order-inv-out")
    @Transactional(type = RW)
    @PartitionBy(clazz = NewOrderWareOut.class, method = "getId")
    public NewOrderInvOut processNewOrder(NewOrderWareOut in) {
        // System.out.println(STR."received NewOrderWareOut in inventory \{in}");

        if (in.mustAbort == 1) {
            throw new RuntimeException("Abort processing of new-order-inv-out");
        }

        int n = in.itemsIds.length;

        float[] prices = itemRepository.getPricePerItemId(in.itemsIds);

        if (prices.length != n) {
            System.out.println("not all items have prices");
            throw new RuntimeException("not all items have prices");
        }

        String[] ol_dist_info = new String[n];
        List<Stock> stockItemsToUpdate = new ArrayList<>(n);

        for (int i = 0; i < n; i++)
        {
            var stockId = new Stock.StockId(in.itemsIds[i], in.supWares[i]);
            // System.out.println(STR."InventoryService stockId \{stockId}");
            Stock stock = this.stockRepository.lookupByKey(stockId);
            if (stock == null) {
                System.out.println("stock null for item " + in.itemsIds[i] + " sup " + in.supWares[i]);
                throw new RuntimeException("stock null");
            }

            ol_dist_info[i] = stock.getDistInfo(in.d_id);
            if (ol_dist_info[i] == null) ol_dist_info[i] = "";

            int ol_quantity = in.qty[i];
            if (stock.s_quantity > ol_quantity) {
                stock.s_quantity -= ol_quantity;
            } else {
                stock.s_quantity = stock.s_quantity - ol_quantity + 91;
            }

            stockItemsToUpdate.add(stock);
        }

        try {
            this.stockRepository.updateAll(stockItemsToUpdate);
        } catch (Exception e) {
            System.out.println("updateAll failed");
            throw new RuntimeException("updateAll failed");
        }

        return new NewOrderInvOut(
                in.w_id,
                in.d_id,
                in.c_id,
                in.itemsIds,
                in.supWares,
                in.qty,
                in.allLocal,
                in.w_tax,
                in.d_next_o_id,
                in.d_tax,
                in.c_discount,
                prices,
                ol_dist_info,
                in.mustAbort
        );
    }

}
