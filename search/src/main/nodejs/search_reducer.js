'use strict';

const {getIntermediateSearchResultsKey, getIntermediateSearchResultsPrefix, getSearchMaskId, getSearchResultsKey} = require('./searchutils');
const {getObject, sleep, getAllKeys, streamObject, removeKey, DEBUG} = require('./utils');
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

const reduceResults = async (searchId, allBatchResults, batchResults) => {
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
    const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(fullSearchInputName);

    // Fetch all keys
    const allKeys  = await getAllKeys({
        Bucket: bucket,
        Prefix: intermediateSearchResultsPrefix
    });
    const allBatchResultsKeys = new Set(allKeys);

    let allBatchResults = {};
    let fail = [];
    for(let batchIndex = 0; batchIndex < numBatches; batchIndex++) {
        const batchResultsKey = getIntermediateSearchResultsKey(fullSearchInputName, batchIndex);

        const batchResults = await getObject(bucket, batchResultsKey, null);
        if (batchResults == null) {
            fail.push(batchResultsKey);
            continue;
        }
        if (DEBUG) console.log(batchResults);
        await reduceResults(searchId, allBatchResults, batchResults);
    }

    const numRetry = 5;
    let retry_interval = 500;
    for (let i = 0; i < numRetry; i++)
    {
        if (fail.length > 0) {
            await sleep(retry_interval);
            console.log(`Retry: `, fail.length);
            retry_interval *= 2;
        }
        else
            break;

        let fail_tmp = [];
        for(let i = 0; i < fail.length; i++) {
            const batchResults = await getObject(bucket, fail[i], null);
            if (batchResults == null) {
                fail_tmp.push(fail[i]);
                continue;
            }
            if (DEBUG) console.log(batchResults);
            await reduceResults(searchId, allBatchResults, batchResults);
        }
        fail = fail_tmp;
    }

    if (fail.length > 0) {
        const e = `Error getting object: ${fail}`;
        console.error(e);
        throw new Error(e);
    }

    const nTotalMatches = Object.values(allBatchResults).map(rsByMask => {
        return rsByMask.results.length;
    }).reduce((a, n) => a  + n, 0);

    const allMatches = Object.values(allBatchResults).map(rsByMask => {
        const results = rsByMask.results;
        console.log(`Sort ${results.length} for ${rsByMask.maskId}`);
        results.sort((r1, r2) => r2.matchingPixels - r1.matchingPixels);
        if (maxResultsPerMask && maxResultsPerMask > 0 && results.length > maxResultsPerMask) {
            rsByMask.results = results.slice(0, maxResultsPerMask);
        }
        return rsByMask;
    });

    // write down the results
    const outputUri = await streamObject(
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
