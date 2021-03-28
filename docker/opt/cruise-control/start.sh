#!/bin/bash
set -e
cd /opt/cruise-control

# Set heap memory settings for container environments
if [ -z "$KAFKA_HEAP_OPTS" ]; then
  export KAFKA_HEAP_OPTS="-XX:InitialRAMPercentage=30 -XX:MaxRAMPercentage=70 -XX:MinRAMPercentage=80"
fi
# disable JEP 396: Strongly Encapsulate JDK Internals by Default (JDK-8256299)
export KAFKA_HEAP_OPTS="--illegal-access=permit ${KAFKA_HEAP_OPTS}"
/bin/bash ${DEBUG:+-x} /opt/cruise-control/kafka-cruise-control-start.sh /opt/cruise-control/config/cruisecontrol.properties 8090
