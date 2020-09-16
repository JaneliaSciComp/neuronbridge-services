'use strict';

const {getIntermediateSearchResultsKey, getIntermediateSearchResultsPrefix, getSearchMaskId, getSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const {getObjectWithRetry, putObjectWithRetry, removeKey, DEBUG} = require('./utils');
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
    const maxResultsPerMask =  event.maxResultsPerMask;

    const fullSearchInputName = `${searchInputFolder}/${searchInputName}`;
    let allBatchResults = {};
    for(let batchIndex = 0; batchIndex < numBatches; batchIndex++) {
        const batchResultsKey = getIntermediateSearchResultsKey(fullSearchInputName, batchIndex);
        const batchResults = await getObjectWithRetry(bucket, batchResultsKey, 3);
        if (DEBUG) console.log(batchResults);
        try {
            batchResults.forEach(batchResult => {
                if (allBatchResults[batchResult.maskId]) {
                    allBatchResults[batchResult.maskId] = mergeResults(allBatchResults[batchResult.maskId], batchResult);
                } else {
                    allBatchResults[batchResult.maskId] = batchResult;
                }
            });
        } catch (e) {
            // write down the error
            await updateSearchMetadata({
                id: searchId,
                errorMessage: e.name + ': ' + e.message
            });
            // rethrow the error
            throw e;
        }
    }

    const nTotalMatches = Object.values(allBatchResults).map(rsByMask => {
        return rsByMask.results.length;
    }).reduce((a, n) => a  + n, 0);

    const allMatches = Object.values(allBatchResults).map(rsByMask => {
        const results = rsByMask.results;
        console.log(`Sort ${results.length} for ${maskId}`);
        results.sort((r1, r2) => r2.matchingPixels - r1.matchingPixels);
        if (maxResultsPerMask && maxResultsPerMask > 0 && results.length > maxResultsPerMask) {
            rsByMask.results = results.slice(0, maxResultsPerMask);
        }
        return rsByMask;
    });

    // write down the results
    const outputUri = await putObjectWithRetry(
        bucket,
        getSearchResultsKey(fullSearchInputName),
        allMatches.length > 1
            ? allMatches
            : (allMatches[0]
                ? allMatches[0]
                : {
                    maskId: getSearchMaskId(searchInputName),
                    results: []
                  }),
        3
    );
    console.log(`Saved ${allMatches.length} matches to ${outputUri}`);

    // write down the progress - done
    const now = new Date()
    await updateSearchMetadata({
        id: searchId,
        step: SEARCH_COMPLETED,
        nTotalMatches: nTotalMatches,
        cdsFinished: now.toISOString()
    });

    if (!DEBUG) {
        const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(fullSearchInputName);
        await removeKey(bucket, intermediateSearchResultsPrefix);
    }
    return event;
}
