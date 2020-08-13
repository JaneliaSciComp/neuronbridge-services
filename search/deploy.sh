#!/bin/sh

deployEnv=$1

if [[ ${deployEnv} == "" ]] ; then
    deployEnv=dev
fi

mvn  -DskipTests=true clean package \
    && npm run deployStage -- ${deployEnv}
