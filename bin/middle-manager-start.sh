#!/bin/bash
#

. ./env.sh
if [ ! -f "./.deps" ];
then
    echo "We are not in the bin directory"
    exit 1
fi
if [ ! -f "${_JAVA}" ];
then
    echo "${_JAVA}: command not found"
    exit 1
fi
build_root=$(dirname "$0")
"${_JAVA}" -Xms256M -Xmx2G -server \
 -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 \
 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true -Dcom.sun.management.jmxremote \
 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
 -Dkafka.logs.dir="${build_root}/log" -Dlog4j.configuration=file:"${build_root}/conf/log4j.properties" \
 -Dfile.encoding=UTF-8 org.demo.kafka.tools.qf2.MiddleManager "${build_root}/conf/middle-manager-restapi.properties"
