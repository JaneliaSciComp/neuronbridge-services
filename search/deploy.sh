#!/bin/sh
#export SLS_DEBUG=*

deployEnv=$1

if [[ ${deployEnv} == "" ]] ; then
    deployEnv=dev
fi

mvn  -DskipTests=true clean package \
    && serverless deploy -s ${deployEnv}
