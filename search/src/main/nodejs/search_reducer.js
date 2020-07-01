'use strict';

const {getIntermediateSearchResultsKey, getSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const {getObject, putText, putObject, removeKey, DEBUG} = require('./utils');

const mergeResults = (rs1, rs2) => {
    if (rs1.maskId === rs2.maskId) {
        return {
            maskId: rs1.maskId,
            results: [...rs1.results, ...rs2.results]
        };
    } else {
        console.log(`Results could not be merged because ${rs1.maskId} is different from  ${rs2.maskId}`);
        throw new Error(`Results could not be merged because ${rs1.maskId} is different from  ${rs2.maskId}`);
    }
}

exports.searchReducer = async (event, context) => {
    // Parameters
    if (DEBUG) console.log(event);
    const bucket = event.bucket;
    const searchInputName = event.searchInputName;
    const numBatches = event.numBatches;

    let allBatchResults = {};
    for(let batchIndex = 0; batchIndex < numBatches; batchIndex++) {
        const batchResultsKey = getIntermediateSearchResultsKey(searchInputName, batchIndex);
        const batchResults = await getObject(bucket, batchResultsKey);
        if (DEBUG) console.log(batchResults);
        batchResults.forEach(batchResult => {
            if (allBatchResults[batchResult.maskId]) {
                allBatchResults[batchResult.maskId] = mergeResults(allBatchResults[batchResult.maskId], batchResult);
            } else {
                allBatchResults[batchResult.maskId] = batchResult;
            }
        });
    }

    const allMatches = Object.values(allBatchResults).map(rsByMask => {
        console.log(`Sort ${rsByMask.results.length} for ${rsByMask.maskId}`);
        rsByMask.results.sort((r1, r2) => r2.matchingPixels - r1.matchingPixels);
        return rsByMask;
    });

    // write down the results
    const outputUri = await putObject(
        bucket,
        getSearchResultsKey(searchInputName),
        allMatches.length > 1 ? allMatches : allMatches[0]
    );
    console.log(`Saved ${allMatches.length} matches to ${outputUri}`);

    // write down the progress - done
    putText(bucket, getSearchProgressKey(searchInputName), "100");

    if (!DEBUG) {
        const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(searchInputName);
        await removeKey(bucket, intermediateSearchResultsPrefix);
    }
}
