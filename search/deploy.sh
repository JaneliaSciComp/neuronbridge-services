#!/bin/sh
#export SLS_DEBUG=*

mvn package \
    && serverless deploy -s cgdev
