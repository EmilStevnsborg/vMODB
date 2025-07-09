
## Running only the proxy project

Run the project with the following command:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/flightScheduler-proxy-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Starting transaction
```
curl -X POST -H "Content-Type: application/json" -d '{"customerId":"1", "flightId": "1", "seatNumber": "1A" }' http://localhost:8766
```