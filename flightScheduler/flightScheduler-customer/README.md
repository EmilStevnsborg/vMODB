## Running the project

Run the project with the command below:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/flightScheduler-customer-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Playing with the APIs

Create customer with POST
```
curl -X POST -H "Content-Type: application/json" -d '{"customerId": "1", "name": "John Doe" }' http://localhost:8769/customer
```

Get customer based on id with GET
```
curl -X GET http://localhost:8769/customer/1
```