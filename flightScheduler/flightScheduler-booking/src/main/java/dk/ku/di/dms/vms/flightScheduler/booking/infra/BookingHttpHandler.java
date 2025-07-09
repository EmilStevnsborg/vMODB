package dk.ku.di.dms.vms.flightScheduler.booking.infra;

import dk.ku.di.dms.vms.flightScheduler.booking.repositories.IBookingRepository;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;

public class BookingHttpHandler extends DefaultHttpHandler
{
    private final IBookingRepository repository;
    public BookingHttpHandler(ITransactionManager transactionManager,
                             IBookingRepository repository){
        super(transactionManager);
        this.repository = repository;
        System.out.println(STR."\nRepo is \{repository}\n");
    }

    // http://host/booking
    @Override
    public String getAsJson(String uri) {
        String[] split = uri.split("/");
        this.transactionManager.beginTransaction(0, 0, 0,true);
        var bookings = this.repository.getAll();
        return bookings.toString();
    }
}
