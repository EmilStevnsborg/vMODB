package dk.ku.di.dms.vms.flightScheduler.booking.repositories;

import dk.ku.di.dms.vms.flightScheduler.booking.entities.Booking;
import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

import java.util.List;

public interface IBookingRepository extends IRepository<Integer, Booking> {}
