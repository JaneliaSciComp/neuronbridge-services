'use strict';

import {getIntermediateSearchResultsKey} from "./searchutils";
import {getObject, putText, putObject, DEBUG} from "./utils";

const {getSearchResultsKey, getSearchProgressKey} = require('./searchutils');

export const searchReducer = async (event, context) => {
    // Parameters
    if (DEBUG) console.log(event);
    const bucket = event.bucket;
    const searchInputKey = event.searchInputKey;
    const numBatches = event.numBatches;

    const allMatches = [];
    for(let batchIndex = 0; batchIndex < numBatches; i++) {
        const batchResultsKey = getIntermediateSearchResultsKey(searchInputKey, batchIndex);
        const batchResults = await getObject(bucket, batchResultsKey);
        if (DEBUG) console.log(batchResults);
        allMatches.push(batchResults); // FIXME
    }

    // write down the results
    const outputUri = await putObject(bucket, getSearchResultsKey(searchInputKey), allMatches);
    console.log(`Saved matches to ${outputUri}`);

    // write down the progress - done
    putText(bucket, getSearchProgressKey(searchInputKey), "100");
}
