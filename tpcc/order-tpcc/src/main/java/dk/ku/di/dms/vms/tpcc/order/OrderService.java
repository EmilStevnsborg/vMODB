package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Parallel;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderInvOut;
import dk.ku.di.dms.vms.tpcc.order.entities.NewOrder;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("order")
public final class OrderService {

    private final IOrderRepository orderRepository;
    private final INewOrderRepository newOrderRepository;
    private final IOrderLineRepository orderLineRepository;

    public OrderService(IOrderRepository orderRepository, INewOrderRepository newOrderRepository, IOrderLineRepository orderLineRepository) {
        this.orderRepository = orderRepository;
        this.newOrderRepository = newOrderRepository;
        this.orderLineRepository = orderLineRepository;
    }

    @Inbound(values = "new-order-inv-out")
    @Transactional(type = W)
    @Parallel
    public void processNewOrder(NewOrderInvOut in){

        Order order = new Order(
                in.d_next_o_id,
                in.d_id,
                in.w_id,
                in.c_id,
                new Date(),
                -1, // set in delivery tx
                in.itemsIds.length,
                in.allLocal ? 1 : 0
        );

        NewOrder newOrder = new NewOrder(in.d_next_o_id, in.d_id, in.w_id);

        this.orderRepository.insert(order);
        this.newOrderRepository.insert(newOrder);

        List<OrderLine> orderLinesToInsert = new ArrayList<>(in.itemsIds.length);

        for(int i = 0; i < in.itemsIds.length; i++){
            float ol_amount = (float) (in.qty[i] * in.itemsIds[i] * (1 + in.w_tax + in.d_tax) * (1 - in.c_discount));
            OrderLine orderLine = new OrderLine(
                    in.d_next_o_id,
                    in.d_id,
                    in.w_id,
                    i+1,
                    in.itemsIds[i],
                    in.supWares[i],
                    null,
                    in.qty[i],
                    ol_amount,
                    in.ol_dist_info[i]
            );
            orderLinesToInsert.add(i, orderLine);
        }
        this.orderLineRepository.insertAll(orderLinesToInsert);
    }

}
