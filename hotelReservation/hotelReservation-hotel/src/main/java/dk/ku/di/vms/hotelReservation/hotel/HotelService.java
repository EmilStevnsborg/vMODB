package dk.ku.di.vms.hotelReservation.hotel;

import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.vms.hotelReservation.hotel.repositories.IProfileRepository;
import dk.ku.di.vms.hotelReservation.hotel.repositories.IRateRepository;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("hotel")
public class HotelService
{
    private final IProfileRepository profileRepository;
    private final IRateRepository rateRepository;

    public HotelService(IProfileRepository profileRepository, IRateRepository rateRepository) {
        this.profileRepository = profileRepository;
        this.rateRepository = rateRepository;
    }

//    @Inbound(values = {ORDER_FLIGHT})
//    @Outbound(BOOK_SEAT)
//    @Transactional(type=RW)
}
