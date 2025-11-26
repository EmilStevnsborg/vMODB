package dk.ku.di.dms.vms.flightScheduler.payment.repositories;

import dk.ku.di.dms.vms.flightScheduler.payment.entities.Payment;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

public interface IPaymentRepository  extends IRepository<Integer, Payment> {}
