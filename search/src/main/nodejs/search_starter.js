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
    let sourceIsHttpApi;
    let eventBody;
    if (event.body) {
        eventBody = JSON.parse(event.body);
        console.log("Parsed body", eventBody)
        sourceIsHttpApi = true;
    } else {
        eventBody = event;
        sourceIsHttpApi = false;
    }
    const newRecords = await getNewRecords(eventBody);
    const searchPromises = await newRecords.map(async r => await startColorDepthSearch(r));
    const results = await Promise.all(searchPromises);
    return results;
};
