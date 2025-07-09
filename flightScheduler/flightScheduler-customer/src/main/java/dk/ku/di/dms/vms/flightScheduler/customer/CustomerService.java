package dk.ku.di.dms.vms.flightScheduler.customer;

import dk.ku.di.dms.vms.flightScheduler.common.events.SeatBooked;
import dk.ku.di.dms.vms.flightScheduler.customer.entities.Customer;
import dk.ku.di.dms.vms.flightScheduler.customer.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;

@Microservice("customer")
public class CustomerService
{
    private static final System.Logger LOGGER = System.getLogger(CustomerService.class.getName());
    private final ICustomerRepository customerRepository;
    public CustomerService(ICustomerRepository customerRepository){
        this.customerRepository = customerRepository;
    }

    @Inbound(values = {SEAT_BOOKED})
    @Transactional(type=R)
    public void seatBookedConfirmed(SeatBooked seatBooked)
    {
//        System.out.println(STR."seatBooked: \{seatBooked.toString()}");

        Customer customer = this.customerRepository.lookupByKey(seatBooked.customerId );
        if(customer == null){
            throw new RuntimeException(STR."Customer \{seatBooked.customerId} cannot be found!");
        }
        System.out.println(STR."\{customer.name} with id \{customer.customerId} has booked "
                           + STR."flight \{seatBooked.flightId} seat \{seatBooked.seatNumber}");
    }
}
