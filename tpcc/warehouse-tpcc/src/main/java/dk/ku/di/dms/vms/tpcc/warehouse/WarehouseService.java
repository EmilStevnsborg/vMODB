package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareOut;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("warehouse")
public final class WarehouseService {

    private final IWarehouseRepository warehouseRepository;
    private final IDistrictRepository districtRepository;
    private final ICustomerRepository customerRepository;

    public WarehouseService(IWarehouseRepository warehouseRepository, IDistrictRepository districtRepository, ICustomerRepository customerRepository){
        this.warehouseRepository = warehouseRepository;
        this.districtRepository = districtRepository;
        this.customerRepository = customerRepository;
    }

    @Inbound(values = "new-order-ware-in")
    @Outbound("new-order-ware-out")
    @Transactional(type = RW)
    @PartitionBy(clazz = NewOrderWareIn.class, method = "getId")
    public NewOrderWareOut processNewOrder(NewOrderWareIn in) {

        District district = this.districtRepository.lookupByKey(new District.DistrictId(in.d_id, in.w_id));
        float w_tax = this.warehouseRepository.getWarehouseTax(in.w_id);

        district.d_next_o_id++;
        this.districtRepository.update(district);

        float c_discount = this.customerRepository.getDiscount(in.w_id, in.d_id, in.c_id);

        return new NewOrderWareOut(
                in.w_id,
                in.d_id,
                in.c_id,
                in.itemsIds,
                in.supWares,
                in.qty,
                in.allLocal,
                w_tax,
                district.d_next_o_id,
                district.d_tax,
                c_discount
        );
    }

}