#!/bin/bash
#

find ./ -name "*.jar" > /tmp/.cp

_CLASSPATH="./"
while read _cp || [[ -n "$_cp" ]];
do
    _CLASSPATH="${_CLASSPATH}:${_cp}"
done < /tmp/.cp;
unset CLASSPATH
export CLASSPATH="${_CLASSPATH}"
echo "CLASSPATH=$CLASSPATH"
unset JAVA_HOME
if [ -f "/usr/libexec/java_home" ];
then
  export JAVA_HOME=$(/usr/libexec/java_home)
else
  export JAVA_HOME="/usr/lib/jvm/java-8-oracle"
fi
echo "JAVA_HOME=$JAVA_HOME"
unset JRE_HOME
export JRE_HOME="${JAVA_HOME}/jre"
if [ ! -d "${JRE_HOME}" ];
then
    unset JRE_HOME
    export JRE_HOME="/usr/lib/jvm/java-8-oracle/jre"
fi
echo "JRE_HOME=$JRE_HOME"
unset _JAVA
export _JAVA="${JAVA_HOME}/bin/java"
echo "_JAVA=$_JAVA"
unset _JAVAC
export _JAVAC="${JAVA_HOME}/bin/javac"
echo "_JAVAC=$_JAVAC"
unset _JAR
export _JAR="${JAVA_HOME}/bin/jar"
echo "_JAR=$_JAR"
unset _MVN
if [ -f "$(ls /usr/local/Cellar/maven/*/libexec/bin/mvn)" ];
then
  export _MVN=$(ls /usr/local/Cellar/maven/*/libexec/bin/mvn)
else
  export _MVN="/usr/bin/mvn"
fi
echo "_MVN=$_MVN"
