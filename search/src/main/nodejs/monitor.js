'use strict';

const moment = require('moment');

const {getIntermediateSearchResultsPrefix, getIntermediateSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const {getAllKeys, putText, DEBUG} = require('./utils');
const {updateSearchMetadata} = require('./awsappsyncutils');

const SEARCH_TIMEOUT_SECS = process.env.SEARCH_TIMEOUT_SECS;

exports.isSearchDone = async (event, context) => {

    // Parameters
    if (DEBUG) console.log(event);
    const bucket = event.bucket;
    const searchId = event.searchId;
    const searchInputFolder = event.searchInputFolder;
    const searchInputName = event.searchInputName;
    const numBatches = event.numBatches;
    const startTime = moment(event.startTime);

    if (!bucket) {
        throw new Error('Missing required key \'bucket\' in input to isSearchDone');
    }

    if (!searchInputName) {
        throw new Error('Missing required key \'searchInputName\' in input to isSearchDone');
    }

    const fullSearchInputName = `${searchInputFolder}/${searchInputName}`;
    const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(fullSearchInputName);

    // Fetch all keys
    const allKeys  = await getAllKeys({
        Bucket: bucket,
        Prefix: intermediateSearchResultsPrefix
    });
    const allBatchResultsKeys = new Set(allKeys);

    // Check if all partitions have completed
    let numComplete = 0;
    for(let batchIndex = 0; batchIndex < numBatches; batchIndex++) {
        const batchResultsKey = getIntermediateSearchResultsKey(fullSearchInputName, batchIndex);
        if (allBatchResultsKeys.has(batchResultsKey)) {
            numComplete++;
        }
    }

    console.log(`Completed: ${numComplete}/${numBatches}`);

    // Calculate total search time
    const now = new Date();
    const endTime = moment(now.toISOString());
    const elapsedSecs = endTime.diff(startTime, "s");
    const numRemaining = numBatches - numComplete;
    // write down the progress
    await updateSearchMetadata({
        id: searchId,
        step: 3,
        completedBatches: numComplete
    });
    // return result for next state input
    if (numRemaining === 0) {
        console.log(`Search took ${elapsedSecs} seconds`);
        return {
            ...event,
            elapsedSecs: elapsedSecs,
            numRemaining: 0,
            completed: true,
            timedOut: false
        };
    } else if (elapsedSecs > SEARCH_TIMEOUT_SECS) {
        console.log(`Search timed out after ${elapsedSecs} seconds`);
        return {
            ...event,
            elapsedSecs: elapsedSecs,
            numRemaining: numRemaining,
            completed: false,
            timedOut: true
        };
    } else {
        console.log(`Search still running after ${elapsedSecs} seconds. Completed ${numComplete} of ${numBatches} jobs.`);
        return {
            ...event,
            elapsedSecs: elapsedSecs,
            numRemaining: numRemaining,
            completed: false,
            timedOut: false
        };
    }

}