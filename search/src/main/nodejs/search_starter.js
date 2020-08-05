'use strict';

const {getSearchMetadata, invokeAsync} = require('./utils');
const dispatchFunction = process.env.SEARCH_DISPATCH_FUNCTION;

const startColorDepthSearch = async (searchParams) => {
    console.log('Start ColorDepthSearch', searchParams);
    // update the step
    const now = new Date();
    const dispatchSearchParams = {
        level: 0,
        numLevels: 2,
        ...searchParams
    };
    const cdsInvocationResult = await invokeAsync(dispatchFunction, dispatchSearchParams);
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
    newRecords.forEach(r => {
        startColorDepthSearch(r);
    });
};

