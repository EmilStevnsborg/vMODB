package dk.ku.di.dms.vms.flightScheduler.payment;

import dk.ku.di.dms.vms.flightScheduler.common.events.*;
import dk.ku.di.dms.vms.flightScheduler.payment.entities.Payment;
import dk.ku.di.dms.vms.flightScheduler.payment.entities.ReimbursableBooking;
import dk.ku.di.dms.vms.flightScheduler.payment.repositories.IPaymentRepository;
import dk.ku.di.dms.vms.flightScheduler.payment.repositories.IReimbursableBookingRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.flightScheduler.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("payment")
public class PaymentService
{
    private static final System.Logger LOGGER = System.getLogger(PaymentService.class.getName());
    private static int booking_counter = 0;
    private final IPaymentRepository paymentRepository;
    private final IReimbursableBookingRepository reimbursableBookingRepository;
    public PaymentService(IPaymentRepository paymentRepository,
                          IReimbursableBookingRepository reimbursableBookingRepository)
    {
        this.paymentRepository = paymentRepository;
        this.reimbursableBookingRepository = reimbursableBookingRepository;
    }

    // part of PayBooking
    @Inbound(values = {PAY_BOOKING})
    @Outbound(PAYMENT_SUCCEEDED)
    @Transactional(type=W)
    public PaymentSucceeded payBooking(PayBooking payBooking)
    {
        var payment = new Payment(payBooking.booking_id, payBooking.payment_method);
        paymentRepository.insert(payment);

        var paymentSucceeded = new PaymentSucceeded(payBooking.booking_id);
        return paymentSucceeded;
    }

    // part of CancelBooking
    @Inbound(values = {BOOKING_CANCELLED})
    @Transactional(type=W)
    public void reimbursePayment(BookingCancelled bookingCancelled)
    {
        if (bookingCancelled.requires_reimbursement == 1)
        {
            var reimbursableBooking = new ReimbursableBooking(bookingCancelled.booking_id, bookingCancelled.customer_id);
            this.reimbursableBookingRepository.insert(reimbursableBooking);
        }
        paymentRepository.deleteByKey(bookingCancelled.booking_id);
    }

    // part of ReimburseBooking
    @Inbound(values = {REIMBURSE_BOOKING})
    @Outbound(CUSTOMER_REIMBURSED)
    @Transactional(type=RW)
    public CustomerReimbursed reimburseBooking(ReimburseBooking reimburseBooking)
    {
        var reimbursableBooking = reimbursableBookingRepository.lookupByKey(reimburseBooking.booking_id);

        var customerReimbursed = new CustomerReimbursed(reimbursableBooking.customer_id);
        reimbursableBookingRepository.deleteByKey(reimburseBooking.booking_id);
        return customerReimbursed;
    }
}
