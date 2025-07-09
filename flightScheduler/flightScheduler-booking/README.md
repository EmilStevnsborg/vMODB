## Running the project

Run the project with the command below:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/flightScheduler-booking-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Playing with the APIs

Get all bookings
```
curl -X GET http://localhost:8768/booking
```