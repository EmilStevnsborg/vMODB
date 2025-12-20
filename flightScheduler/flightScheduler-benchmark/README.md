
Run the benchmark experiments with the following command. Look in the main file to modify which experiment is run. Check the experiment files to modify the experiment configurations.
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/flightScheduler-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar
```