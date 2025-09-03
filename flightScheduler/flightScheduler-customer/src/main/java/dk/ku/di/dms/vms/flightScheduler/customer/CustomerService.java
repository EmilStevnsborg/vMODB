package dk.ku.di.dms.vms.flightScheduler.customer;

import dk.ku.di.dms.vms.flightScheduler.common.events.CustomerReimbursed;
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

    // part of OrderFlight
    @Inbound(values = {SEAT_BOOKED})
    @Transactional(type=R)
    public void seatBookedConfirmed(SeatBooked seatBooked)
    {
        Customer customer = this.customerRepository.lookupByKey(seatBooked.customer_id );
        if(customer == null){
            throw new RuntimeException(STR."Customer \{seatBooked.customer_id} cannot be found!");
        }
        System.out.println(STR."\{customer.name} with id \{customer.customer_id} has booked "
                           + STR."flight \{seatBooked.flight_id} seat \{seatBooked.seat_number}");
    }

    // part of ReimburseBooking
    @Inbound(values = {CUSTOMER_REIMBURSED})
    @Transactional(type=R)
    public void bookingReimbursed(CustomerReimbursed customerReimbursed)
    {
        Customer customer = this.customerRepository.lookupByKey(customerReimbursed.customer_id );
        System.out.println(STR."\{customer.name} with id \{customer.customer_id} has been reimburdsed");
    }
}
