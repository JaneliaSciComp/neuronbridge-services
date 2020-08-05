'use strict';

const {invokeAsync} = require('./utils');
const {getSearchMetadata} = require('./awsappsyncutils');

const dispatchFunction = process.env.SEARCH_DISPATCH_FUNCTION;

const startColorDepthSearch = async (searchParams) => {
    console.log('Start ColorDepthSearch', searchParams);
    const cdsInvocationResult = await invokeAsync(dispatchFunction, searchParams);
    console.log('Started ColorDepthSearch', cdsInvocationResult);
    return cdsInvocationResult;
}

const getNewRecords = async (e) => {
    if (e.Records) {
        const newRecordsPromises = await e.Records
            .filter(r => r.eventName === 'INSERT')
            .map(r => r.dynamodb)
            .map(r => r.Keys.id.S)
            .map(async searchId => await getSearchMetadata(searchId));
        return await Promise.all(newRecordsPromises);
    } else if (e.searchIds) {
        const newSearchesPromises = await e.searchIds
            .map(async searchId => await getSearchMetadata(searchId));
        return await Promise.all(newSearchesPromises);
    } else if (e.searches) {
        return e.searches;
    } else {
        return [];
    }
}

exports.searchStarter = async (event) => {
    console.log(event);
    const newRecords = await getNewRecords(event);
    const searchPromises = await newRecords.map(async r => await startColorDepthSearch(r));
    return await Promise.all(searchPromises);
};

