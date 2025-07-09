

1. Init `ORDER_FLIGHT` event from http call in coordinator with u_id
2. Flight Service picks up the `ORDER_FLIGHT`, reserves a seat for the flightSeat and emits `RESERVE_BOOKING`
3. Booking Service picks up `RESERVE_BOOKING`, stores the booking, emits `FLIGHT_BOOKED`
4. Customer Service picks up `FLIGHT_BOOKED`, sends email to user who's booking was completed

Use ad-hoc queries to register users and flights

