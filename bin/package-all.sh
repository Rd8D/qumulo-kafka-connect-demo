#!/bin/bash
#

. ./env.sh

if [ ! -f "${_JAR}" ];
then
    echo "${_JAR}: command not found"
    exit 1
fi
if [ ! -f "./.deps" ];
then
    echo "We are not in the build directory"
    exit 1
fi
build_root=$(dirname "$0")
if [ -f "${build_root}/qumulo-kafka-connect-demo.jar" ];
then    
    read -p "Deleting ${build_root}/qumulo-kafka-connect-demo.jar? (y/n) " answer
    if [ "$answer" = "y" ]; then
        rm -f "${build_root}/qumulo-kafka-connect-demo.jar"
    else
        exit 0
    fi
fi
echo "Manifest-Version: 1.0" > /tmp/.manifest
echo "Main-Class: org.demo.kafka.connect.qf2.ConnectStandalone" >> /tmp/.manifest
firstl=true
while read _cp || [[ -n "$_cp" ]];
do
  if [ "$firstl" = true ] ; then
    echo "Class-Path: ${_cp}" >> /tmp/.manifest
    firstl=false
  else
    echo " ${_cp}" >> /tmp/.manifest
  fi
done < /tmp/.cp;
"${_JAR}" cvfm qumulo-kafka-connect-demo.jar /tmp/.manifest conf lib org



