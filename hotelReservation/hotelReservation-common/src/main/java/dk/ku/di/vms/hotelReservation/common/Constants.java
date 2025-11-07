package dk.ku.di.vms.hotelReservation.common;

public class Constants
{
    public static final int PROXY_VMS_PORT = 9875;
    public static final int HOTEL_VMS_PORT = 9876;
    public static final int RESERVATION_VMS_PORT = 9877;
    public static final int USER_VMS_PORT = 9878;
    public static final int ATTRACTION_VMS_PORT = 9879;


    // MakeReservation transaction
    public static final String MAKE_RESERVATION = "make_reservation";
    public static final String CREATE_RESERVATION = "create_reservation";
    public static final String CONFIRM_RESERVATION = "confirm_reservation";

    // SubmitReview
    public static final String SUBMIT_REVIEW = "submit_review";
    public static final String STORE_REVIEW = "store_review";

}
