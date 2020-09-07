'use strict';

const {getIntermediateSearchResultsKey, getSearchMaskId, getSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const {getObject, sleep, putText, putObject, removeKey, DEBUG} = require('./utils');
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
    let fail = [];
    for(let batchIndex = 0; batchIndex < numBatches; batchIndex++) {
        const batchResultsKey = getIntermediateSearchResultsKey(fullSearchInputName, batchIndex);
        const batchResults = await getObject(bucket, batchResultsKey, null);
        if (batchResults == null) {
            fail.push(batchResultsKey);
            continue;
        }
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

    if (fail.length > 0) {
        await sleep(500);
        console.log(`SecondTry: `, fail.length);
    }

    let fail2 = [];
    for(let i = 0; i < fail.length; i++) {
        const batchResults = await getObject(bucket, fail[i], null);
        if (batchResults == null) {
            fail2.push(fail[i]);
            continue;
        }
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

    if (fail2.length > 0) {
        await sleep(1000);
        console.log(`ThirdTry: `, fail.length);
    }

    let fail3 = [];
    for(let i = 0; i < fail2.length; i++) {
        const batchResults = await getObject(bucket, fail2[i], null);
        if (batchResults == null) {
            fail3.push(fail2[i]);
            continue;
        }
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

    if (fail3.length > 0) {
        await sleep(2000);
        console.log(`FourthTry: `, fail3.length);
    }

    let fail4 = [];
    for(let i = 0; i < fail3.length; i++) {
        const batchResults = await getObject(bucket, fail3[i], null);
        if (batchResults == null) {
            fail4.push(fail3[i]);
            continue;
        }
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

    if (fail4.length > 0) {
        const e = `Error getting object: ${fail4}`;
        console.error(e);
        throw new Error(e);
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

    // write down the progress - done
    const now = new Date()
    await updateSearchMetadata({
        id: searchId,
        step: SEARCH_COMPLETED,
        cdsFinished: now.toISOString()
    });

    if (!DEBUG) {
        const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(fullSearchInputName);
        await removeKey(bucket, intermediateSearchResultsPrefix);
    }
    return event;
}
