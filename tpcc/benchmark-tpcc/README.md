Run the TPC-C experiments with the following command. Look in the experiment files to modify the experiment configurations.

```
 java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/benchmark-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```