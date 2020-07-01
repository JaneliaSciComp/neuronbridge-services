'use strict';

const {getIntermediateSearchResultsKey, getSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const {getObject, putText, putObject, removeKey, DEBUG} = require('./utils');

const mergeResults = (rs1, rs2) => {
    console.log(`!!!!!MERGE RESULTS`, rs1, "!!!!!!!!!!!@@@@!!!!!!!@@@@@@@", rs2);
    if (rs1.maskId === rs2.maskId) {
        const mergedResult = {
            maskId: rs1.maskId,
            results: [].concat(rs1.results, rs2.results)
        };
        console.log(`!!!!!NEW MERGED RESULTS`, mergedResult);
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
                allBatchResults[batchResult.maskId] = mergeResults(allBatchResults.get(batchResult.maskId), batchResult);
            } else {
                allBatchResults[batchResult.maskId] = batchResult;
            }
        });
    }

    const allMatches = Object.values(allBatchResults).map(rsByMask => {
        console.log(`Sort ${rsByMask.results.length} for ${rsByMask.maskId}`);
        rsByMask.results.sort((r1, r2) => r1.matchingPixels - r2.matchingPixels);
        console.log(`!!!!Sorted ${rsByMask.results.length} for ${rsByMask.maskId}`);
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
