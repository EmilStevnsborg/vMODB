package dk.ku.di.dms.vms.flightScheduler.customer;

import dk.ku.di.dms.vms.flightScheduler.common.events.CustomerPaid;
import dk.ku.di.dms.vms.flightScheduler.common.events.CustomerReimbursed;
import dk.ku.di.dms.vms.flightScheduler.common.events.SeatBooked;
import dk.ku.di.dms.vms.flightScheduler.customer.entities.Customer;
import dk.ku.di.dms.vms.flightScheduler.customer.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

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
    @Transactional(type=RW)
    public void seatBookedConfirmed(SeatBooked seatBooked)
    {
        Customer customer = this.customerRepository.lookupByKey(seatBooked.customer_id);
        if(customer == null){
            throw new RuntimeException(STR."Customer \{seatBooked.customer_id} cannot be found!");
        }
        customer.addSeat(seatBooked.seat_number);
        this.customerRepository.update(customer);
    }


    @Inbound(values = {CUSTOMER_PAID})
    @Transactional(type=RW)
    public void customerPaid(CustomerPaid customerPaid)
    {
        Customer customer = this.customerRepository.lookupByKey(customerPaid.customer_id);
        customer.deduct(customerPaid.price);
        this.customerRepository.update(customer);
    }

    // part of ReimburseBooking
    @Inbound(values = {CUSTOMER_REIMBURSED})
    @Transactional(type=RW)
    public void bookingReimbursed(CustomerReimbursed customerReimbursed)
    {
        Customer customer = this.customerRepository.lookupByKey(customerReimbursed.customer_id);
        customer.money += customerReimbursed.reimbursement;
        this.customerRepository.update(customer);
        System.out.println(STR."\{customer.name} with id \{customer.customer_id} has been reimburdsed with \{customerReimbursed.reimbursement}");
    }
}
