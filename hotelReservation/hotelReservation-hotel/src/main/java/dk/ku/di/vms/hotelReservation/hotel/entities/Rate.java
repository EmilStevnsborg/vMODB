package dk.ku.di.vms.hotelReservation.hotel.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.validation.constraints.Positive;
import java.io.Serializable;
import java.util.Date;

@VmsTable(name="rates")
public class Rate implements IEntity<Rate.RateId>
{
    @Id
    public RateId rate_id;
    @Column
    public double bookableRate;
    @Column
    public double totalRate;
    @Column
    public double totalRateInclusive;
    @Column
    public String code;
    @Column
    public String currency;
    @Column
    public String roomDescription;

    public static class RateId implements Serializable {
        public int hotel_id;
        public Date in_date;
        public Date out_date;
        public RateId() {}
        public RateId(int hotel_id, Date in_date, Date out_date)
        {
            this.hotel_id = hotel_id;
            this.in_date = in_date;
            this.out_date = out_date;
        }

        @Override
        public String toString()
        {
            return STR."Id: { hotel_id=\{hotel_id}, in_date=\{in_date}, out_date=\{out_date} }";
        }
    }

    public Rate() {}

    public Rate(int hotel_id, Date in_date, Date out_date)
    {
        this.rate_id = new RateId(hotel_id, in_date, out_date);
    }
}
