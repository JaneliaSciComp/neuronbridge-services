'use strict';

const {getIntermediateSearchResultsKey, getSearchMaskId, getSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const {getObjectWithRetry, putText, putObject, removeKey, DEBUG} = require('./utils');
const {updateSearchMetadata, SEARCH_COMPLETED} = require('./awsappsyncutils');

const mergeResults = (rs1, rs2) => {
    if (rs1.maskId === rs2.maskId) {
        return {
            maskId: rs1.maskId,
            maskPublishedName: rs1.maskPublishedName,
            maskLibraryName: rs1.maskLibraryName,
            maskImageURL: rs1.maskImageURL,
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
    const searchId = event.searchId;
    const searchInputFolder = event.searchInputFolder;
    const searchInputName = event.searchInputName;
    const numBatches = event.numBatches;

    const fullSearchInputName = `${searchInputFolder}/${searchInputName}`;
    let allBatchResults = {};
    for(let batchIndex = 0; batchIndex < numBatches; batchIndex++) {
        const batchResultsKey = getIntermediateSearchResultsKey(fullSearchInputName, batchIndex);
        const batchResults = await getObjectWithRetry(bucket, batchResultsKey, 3);
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
        getSearchResultsKey(fullSearchInputName),
        allMatches.length > 1
            ? allMatches
            : (allMatches[0]
                ? allMatches[0]
                : {
                    maskId: getSearchMaskId(searchInputName),
                    results: []
                  })
    );
    console.log(`Saved ${allMatches.length} matches to ${outputUri}`);

    if (searchId) {
        // write down the progress - done
        const now = new Date()
        await updateSearchMetadata({
            id: searchId,
            step: SEARCH_COMPLETED,
            cdsFinished: now.toISOString()
        });
    }
    if (!DEBUG) {
        const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(fullSearchInputName);
        await removeKey(bucket, intermediateSearchResultsPrefix);
    }
    return event;
}
