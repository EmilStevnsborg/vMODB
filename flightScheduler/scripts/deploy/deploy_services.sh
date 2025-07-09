#!/bin/bash

# Access individual command-line arguments
echo "Script name: $0"
echo "Number of arguments: $#"
echo "The arguments passed: $*"

> .deploy_windows

help_="--help"
param1="$1"

if [ "$param1" = "$help_" ]; then
    echo "It is expected that the script runs in the marketplace project's root folder."
    echo "You can specify the apps using the following pattern:"
    echo "<app-id1> ... <app-idn>"
    exit 1
fi

var1=1
current_dir=$(pwd)
echo "Current dir is" $current_dir

echo ""

if [ $# -eq 0 ];
then
  echo "ERROR: No arguments passed"
  exit 1
fi

if `echo "$*" | grep -q flight`; then
    s=`ps | grep -c flight`
    if [ $s = $var1 ]
    then
        echo "flight already running"
    else
        echo "initializing flight..."
        WINDOW_INFO=$(osascript -e 'tell app "Terminal"
            do script "java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -Xmx2048m -jar '$current_dir'/flightScheduler-flight/target/flightScheduler-flight-1.0-SNAPSHOT-jar-with-dependencies.jar"
        end tell')
        WINDOW_ID=$(echo "$WINDOW_INFO" | awk '{print $6}')
    fi
fi

if `echo "$*" | grep -q booking`; then
    s=`ps | grep -c booking`
    if [ $s = $var1 ]
    then
        echo "booking already running"
    else
        echo "initializing booking..."
        WINDOW_INFO=$(osascript -e 'tell app "Terminal"
            do script "java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -Xmx2048m -jar '$current_dir'/flightScheduler-booking/target/flightScheduler-booking-1.0-SNAPSHOT-jar-with-dependencies.jar"
        end tell')
        WINDOW_ID=$(echo "$WINDOW_INFO" | awk '{print $6}')
    fi
fi

if `echo "$*" | grep -q customer`; then
    s=`ps | grep -c customer`
    if [ $s = $var1 ]
    then
        echo "customer already running"
    else
        echo "initializing customer..."
        WINDOW_INFO=$(osascript -e 'tell app "Terminal"
            do script "java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -Xmx2048m -jar '$current_dir'/flightScheduler-customer/target/flightScheduler-customer-1.0-SNAPSHOT-jar-with-dependencies.jar"
        end tell')
        WINDOW_ID=$(echo "$WINDOW_INFO" | awk '{print $6}')
    fi
fi