#!/bin/sh
mvn clean install \
    && serverless deploy

