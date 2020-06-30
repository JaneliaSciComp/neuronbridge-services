'use strict';

const {getIntermediateSearchResultsKey, getSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const {getObject, putText, putObject, DEBUG} = require('./utils');

export const searchReducer = async (event, context) => {
    // Parameters
    if (DEBUG) console.log(event);
    const bucket = event.bucket;
    const searchInputName = event.searchInputName;
    const numBatches = event.numBatches;

    const allMatches = [];
    for(let batchIndex = 0; batchIndex < numBatches; i++) {
        const batchResultsKey = getIntermediateSearchResultsKey(searchInputName, batchIndex);
        const batchResults = await getObject(bucket, batchResultsKey);
        if (DEBUG) console.log(batchResults);
        allMatches.push(batchResults); // FIXME
    }

    // write down the results
    const outputUri = await putObject(bucket, getSearchResultsKey(searchInputName), allMatches);
    console.log(`Saved matches to ${outputUri}`);

    // write down the progress - done
    putText(bucket, getSearchProgressKey(searchInputKey), "100");
}
