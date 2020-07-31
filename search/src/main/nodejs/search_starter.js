'use strict';

const AWS = require("aws-sdk");

AWS.config.update({
    apiVersion: '2012-08-10'
});

const startColorDepthSearch = async (searchParams) => {
    console.log('Start ColorDepthSearch', searchParams);
}

const getNewRecords = (e) => {
    return e.Records.filter(r => r.eventName === 'INSERT').map(r => r.dynamodb);
}

exports.searchStarter = async (event) => {
    console.log(event);
    getNewRecords(event)
        .forEach(r => {
            startColorDepthSearch(r.dynamodb);
        });
};

