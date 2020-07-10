#!/bin/sh
#export SLS_DEBUG=*

mvn clean package \
    && serverless deploy -s cgdev
