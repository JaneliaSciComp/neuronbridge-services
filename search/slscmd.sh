#!/bin/sh

cmd=$1
stage=$2

if [[ ${cmd} == "" ]] ; then
    cmd=deploy
fi

if [[ ${stage} == "" ]] ; then
    stage=dev
fi

mvn -DskipTests=true clean package \
    && npm run sls -- deploy -s ${stage}
