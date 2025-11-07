package dk.ku.di.vms.hotelReservation.hotel.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Date;

@VmsTable(name="rates")
public class Review implements IEntity<Review.ReviewId>
{
    @Id
    public Review.ReviewId review_id;
    @Column
    public double bookableRate;

    public static class ReviewId implements Serializable
    {
        public int hotel_id;
        public String username;
        public ReviewId() {}
        public ReviewId(int hotel_id, String username)
        {
            this.hotel_id = hotel_id;
            this.username = username;
        }

        @Override
        public String toString()
        {
            return STR."Id: { hotel_id=\{hotel_id}, username=\{username} }";
        }
    }
}
