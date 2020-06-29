'use strict';

const {getAllKeys, putText, DEBUG} = require('./utils');
const {getIntermediateSearchResultsPrefix, getIntermediateSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const moment = require('moment');

const SEARCH_TIMEOUT_SECS = process.env.SEARCH_TIMEOUT_SECS;

exports.isSearchDone = async (event, context) => {

    // Parameters
    if (DEBUG) console.log(event);
    const bucket = event.bucket;
    const searchInputKey = event.searchInputKey;
    const numBatches = event.numBatches;
    const startTime = moment(event.startTime);

    if (!bucket) {
        throw new Error('Missing required key \'bucket\' in input to isSearchDone');
    }

    if (!searchInputKey) {
        throw new Error('Missing required key \'searchInputKey\' in input to isSearchDone');
    }

    const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(searchInputKey);

    // Fetch all keys
    const allBatchResultsKeys = new Set(await getAllKeys({
        Bucket: bucket,
        Prefix: intermediateSearchResultsPrefix
    }));

    if (DEBUG) console.log(allKeys);

    // Check if all partitions have completed
    let numComplete = 0;
    for(let batchIndex = 0; batchIndex < numBatches; i++) {
        const batchResultsKey = getIntermediateSearchResultsKey(searchInputKey, batchIndex);
        if (allKeys.has(outputKey)) {
            numComplete++;
        }
    }

    // intermediate searches done count for 95% completion, reduce step is the remaining 5%
    const progress = Math.floor(numComplete / numBatches * 95);
    const numRemaining = numBatches - numComplete;
    console.log(`Completed: ${numComplete}/${numBatches}`);

    // Calculate total search time
    const now = new Date();
    const endTime = moment(now.toISOString());
    const elapsedSecs = endTime.diff(startTime, "s");
    // write down the progress
    putText(bucket, getSearchProgressKey(searchInputKey), progress.toString());
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
        console.log(`Search still running after ${elapsedSecs} seconds. Completed ${numComplete} of ${numPartitions} jobs.`);
        return {
            ...event,
            elapsedSecs: elapsedSecs,
            numRemaining: numRemaining,
            completed: false,
            timedOut: false
        };
    }

}