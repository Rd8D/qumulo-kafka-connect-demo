#!/bin/bash
#

. ./env.sh

if [ ! -d "${JAVA_HOME}" ];
then
    echo "${JAVA_HOME}: JAVA_HOME is not set correctly"
    exit 1
fi
if [ ! -f "${_MVN}" ];
then
    echo "${_MVN}: command not found"
    exit 1
fi
has_jq=$(echo '{}' | jq .)
if [ "${has_jq}" != "{}" ];
then
    echo "Command jq not found"
    exit 1
fi
has_wget=$(wget -V | grep -c "GNU Wget")
if [ "${has_wget}" != "1" ];
then
    echo "Command wget not found"
    exit 1
fi
build_root=$(dirname "$0")
if [ ! -f "./.deps" ];
then
    echo "Missing .deps, or the current dir is not the build root"
    exit 1
fi
echo "Downloading all maven dependencies in ${build_root}/lib..."
if [ -d "${build_root}/lib" ];
then    
    read -p "Deleting ${build_root}/lib? (y/n) " answer
    if [ "$answer" = "y" ]; then
        rm -Rf "${build_root}/lib"
    else
        exit 0
    fi
fi
mkdir -p "${build_root}/lib"
if [ ! -d "${JRE_HOME}/lib/" ];
then
    echo "can't find jre/lib directory: ${JRE_HOME}/lib"
    exit 1
fi
cp -Rf "${JRE_HOME}/lib/." "${build_root}/lib"

deps=$(cat "${build_root}/.deps")
while IFS='' read -r dep || [[ -n "$dep" ]];
do
    q=$(echo $dep | cut -d' ' -f1)
    json=$(wget -q -O - http://search.maven.org/solrsearch/select?q=1:${q} | jq .)
    cnt=$(echo $json | jq .response.numFound)
    for (( idx = 0; idx < "$cnt"; idx ++ ))
    do
        id=$(echo $json | jq .response.docs[$idx].id)
        if [ "$id" != "" ];
        then
            jar=$(echo $dep | cut -d' ' -f2)
            id=$(echo "${id}" | sed s/\"//g)
            is_mac=$(uname -s | grep -c ^Darwin)
            if [ "$is_mac" == "1" ];
            then
                echo -e "[DEBUG] $jar resolves to $id"
            else
                echo -e "\e[1m\e[103m[DEBUG]\e[0m $jar resolves to $id"
            fi
            "${_MVN}" org.apache.maven.plugins:maven-dependency-plugin:3.1.1:copy -Dartifact=${id}:jar -DoutputDirectory="${build_root}/lib"
        fi
    done
done < <(printf '%s\n' "$deps")

wget https://repo.maven.apache.org/maven2/javax/transaction/javax.transaction-api/1.3/javax.transaction-api-1.3.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/eclipse/persistence/javax.persistence/2.2.1/javax.persistence-2.2.1.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/connect-api/1.1.0/connect-api-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/connect-file/1.1.0/connect-file-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/connect-json/1.1.0/connect-json-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/connect-runtime/1.1.0/connect-runtime-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/connect-transforms/1.1.0/connect-transforms-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka_2.11/1.1.0/kafka_2.11-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka_2.11/1.1.0/kafka_2.11-1.1.0-javadoc.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka_2.11/1.1.0/kafka_2.11-1.1.0-scaladoc.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka_2.11/1.1.0/kafka_2.11-1.1.0-sources.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka_2.11/1.1.0/kafka_2.11-1.1.0-test.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka_2.11/1.1.0/kafka_2.11-1.1.0-test-sources.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/1.1.0/kafka-clients-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-log4j-appender/1.1.0/kafka-log4j-appender-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-streams/1.1.0/kafka-streams-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-streams/1.1.0/kafka-streams-test-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-tools/1.1.0/kafka-tools-1.1.0.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/easymock/easymock/3.6/easymock-3.6.jar -P "${build_root}/lib"
wget https://repo.maven.apache.org/maven2/org/easymock/easymock/3.6/easymock-3.6-sources.jar -P "${build_root}/lib"

echo "Compiling..."
./compile-all.sh
echo "Packaging..."
./package-all.sh
if [ -f "${build_root}/qumulo-kafka-connect-demo.jar" ];
then
    echo "Build done ٩(- ̮̮̃-̃)۶"
else
    echo "Build failed ٩(̾●̮̮̃̾•̃̾)۶"
fi
