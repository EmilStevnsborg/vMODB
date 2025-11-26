## Running the project

Run the project with the command below:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/flightScheduler-flight-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Playing with the APIs

Create customer with POST
```
curl -X POST -H "Content-Type: application/json" -d '{"flightId": "1", "seatNumber": "1A" }' http://localhost:8767/flight
```
```
curl -X POST -H "Content-Type: application/json" -d '{"flightId": "1", "seatNumber": "1B" }' http://localhost:8767/flight
```
```
curl -X POST -H "Content-Type: application/json" -d '{"flightId": "1", "seatNumber": "2A" }' http://localhost:8767/flight
```

Get customer based on id with GET
```
curl -X GET http://localhost:8767/flightSeat/1
```