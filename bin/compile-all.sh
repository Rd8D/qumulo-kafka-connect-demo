#!/bin/bash
#

. ./env.sh

if [ ! -f "${_JAVAC}" ];
then
    echo "${_JAVAC}: command not found"
    exit 1
fi
if [ ! -f "./.deps" ];
then
    echo "We are not in the build directory"
    exit 1
fi
build_root=$(dirname "$0")
if [ -d "${build_root}/org" ];
then    
    read -p "Deleting ${build_root}/org? (y/n) " answer
    if [ "$answer" = "y" ]; then
        rm -Rf "${build_root}/org"
    else
        exit 0
    fi
fi
if [ -d "${build_root}/conf" ];
then    
    read -p "Deleting ${build_root}/conf? (y/n) " answer
    if [ "$answer" = "y" ]; then
        rm -Rf "${build_root}/conf"
    else
        exit 0
    fi
fi

cp -Rf "${build_root}/../src/." "${build_root}"
cp -Rf "${build_root}/../conf"  "${build_root}/conf"

mkdir -p "${build_root}/var"
rm -f "${build_root}/var/connect.offsets"
offsets="$(cd "$(dirname "${build_root}/var/connect.offsets")" && pwd)/$(basename "${build_root}/var/connect.offsets")";
echo "offset.storage.file.filename=${offsets}" >> "${build_root}/conf/connect-standalone-qf2.properties"
echo "plugin.path=$(pwd)/lib" >> "${build_root}/conf/connect-standalone-qf2.properties"

find "${build_root}/org" -name *.java > "${build_root}/.sources"
"${_JAVAC}" "@${build_root}/.sources"