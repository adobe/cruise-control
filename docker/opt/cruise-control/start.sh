#!/bin/bash
set -e
cd /opt/cruise-control

# Set heap memory settings for container environments
export KAFKA_HEAP_OPTS="--add-opens java.base/java.util=ALL-UNNAMED -XX:InitialRAMPercentage=30 -XX:MaxRAMPercentage=85 \
-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent \
-XX:MetaspaceSize=96m -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 \
-Djava.awt.headless=true -Dsun.net.inetaddr.ttl=60 \
-Dcom.sun.management.jmxremote.port=1090 \
-Dcom.sun.management.jmxremote.rmi.port=1090 \
-Dcom.sun.management.jmxremote.local.only=false \
-Djava.rmi.server.hostname=127.0.0.1 ${KAFKA_HEAP_OPTS}"

/bin/bash ${DEBUG:+-x} /opt/cruise-control/kafka-cruise-control-start.sh /opt/cruise-control/config/cruisecontrol.properties 8090
