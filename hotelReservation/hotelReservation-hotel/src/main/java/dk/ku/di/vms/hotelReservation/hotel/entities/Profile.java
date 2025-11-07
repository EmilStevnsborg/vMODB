package dk.ku.di.vms.hotelReservation.hotel.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.validation.constraints.Positive;

@VmsTable(name="profiles")
public class Profile implements IEntity<Integer>
{
    @Id
    @Positive
    public int hotel_id;
    @Column
    public String name;
    @Column
    public String phone_number;
    @Column
    public String description;
    @Column
    public String address;
    @Column
    public String[] images;
    @Column
    public int capacity;;
    @Column
    public int lon;
    @Column
    public int lat;
    public Profile() {}

    public Profile(int hotel_id, String name,
                   String phone_number, String description,
                   String address, String[] images,
                   int capacity, int lon, int lat)
    {
        this.hotel_id = hotel_id;
        this.name = name;
        this.phone_number = phone_number;
        this.description = description;
        this.address = address;
        this.images = images;
        this.capacity = capacity;
        this.lon = lon;
        this.lat = lat;
    }
}
