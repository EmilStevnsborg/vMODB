package dk.ku.di.vms.hotelReservation.hotel.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.vms.hotelReservation.hotel.entities.Profile;
import dk.ku.di.vms.hotelReservation.hotel.interfaces.HotelDistance;

import java.util.List;

public interface IProfileRepository  extends IRepository<Integer, Profile>
{
    @Query("select hotel_id, (lat-:lat)*(lat-:lat) + (lon-:lon)*(lon-:lon) d from profiles")
    List<HotelDistance> geoDistances(int lon, int lat);
}