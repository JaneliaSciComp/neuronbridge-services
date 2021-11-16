import AWS from 'aws-sdk';
import {getIntermediateSearchResultsPrefix, getSearchMaskId, getSearchResultsKey} from './searchutils';
import {streamObject, removeKey, DEBUG} from './utils';
import {updateSearchMetadata, SEARCH_COMPLETED} from './awsappsyncutils';
import zlib from 'zlib';

var docClient = new AWS.DynamoDB.DocumentClient();

const maxResultsLength = process.env.MAX_CUSTOM_RESULTS || -1;

const mergeBatchResults = async (searchId, items, allBatchResults) => {
    for(const item of items) {
        try {
            extractResults(item).forEach(batchResult => {
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
};

const extractResults = (item) => {
    const resultsSValue = item.resultsMimeType === 'application/gzip'
        ? zlib.gunzipSync(item.results)
        : item.results;
    return JSON.parse(resultsSValue);
};

const mergeResults = (rs1, rs2) => {
    if (rs1.maskId === rs2.maskId) {
        const mergedResults = [...rs1.results, ...rs2.results];
        mergedResults.sort((r1, r2) => r2.matchingPixels - r1.matchingPixels);
        return {
            maskId: rs1.maskId,
            maskPublishedName: rs1.maskPublishedName,
            maskLibraryName: rs1.maskLibraryName,
            maskImageURL: rs1.maskImageURL,
            results: maxResultsLength > 0 ? mergedResults.slice(0, maxResultsLength) : mergedResults
        };
    } else {
        console.log(`Results could not be merged because ${rs1.maskId} is different from  ${rs2.maskId}`);
        throw new Error(`Results could not be merged because ${rs1.maskId} is different from  ${rs2.maskId}`);
    }
};

export const searchCombiner = async (event) => {
    if (DEBUG) console.log('Input event:', JSON.stringify(event));

    // Parameters
    const { jobId, tasksTableName, timedOut, completed, withErrors, fatalErrors } = event;
    const { searchBucket, searchId, maskKeys, maxResultsPerMask }  = event.jobParameters;
    const fullSearchInputName = maskKeys[0];
    const searchInputName = fullSearchInputName.substring(fullSearchInputName.lastIndexOf("/")+1);

    let allBatchResults = {};

    const now = new Date();
    if (fatalErrors && fatalErrors.length > 0) {
        // if there are fatalErrors do not try any reducing step because
        // the process had already been invoked and most likely failed
        // so simply report the error just in case it has not been reported yet
        console.log(`Job ${jobId} - ${searchId} completed with fatal errors`);
        await updateSearchMetadata({
            id: searchId,
            step: SEARCH_COMPLETED,
            errorMessage: "Color depth search completed with fatal errors",
            cdsFinished: now.toISOString()
        });
        return event;
    } else if (timedOut) {
        console.log(`Job ${jobId} - ${searchId} timed out`);
        await updateSearchMetadata({
            id: searchId,
            step: SEARCH_COMPLETED,
            errorMessage: "Color depth search timed out",
            cdsFinished: now.toISOString()
        });
    } else if (withErrors || !completed) {
        console.log(`Job ${jobId} - ${searchId} completed with errors`);
        await updateSearchMetadata({
            id: searchId,
            step: SEARCH_COMPLETED,
            errorMessage: "Color depth search completed with errors",
            cdsFinished: now.toISOString()
        });
    }

    const params = {
        TableName: tasksTableName,
        ConsistentRead: true,
        KeyConditionExpression: 'jobId = :jobId',
        FilterExpression: 'results <> :emptyList',
        ExpressionAttributeValues: {
            ':jobId': jobId,
            ':emptyList': '[]'
        },
      };

    let queryResult;
    do {
        // eslint-disable-next-line no-await-in-loop
        queryResult = await docClient.query(params).promise();
        console.log(`Merging ${queryResult.Items.length} results`, '->', queryResult.LastEvaluatedKey ? queryResult.LastEvaluatedKey : 'end');
        mergeBatchResults(searchId, queryResult.Items, allBatchResults);
        params.ExclusiveStartKey = queryResult.LastEvaluatedKey;
    } while (queryResult.LastEvaluatedKey);

    const matchCounts = Object.values(allBatchResults).map(rsByMask => {
        return rsByMask.results.length;
    });

    const nTotalMatches = matchCounts.reduce((a, n) => a  + n, 0);

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
        searchBucket,
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
    await updateSearchMetadata({
        id: searchId,
        step: SEARCH_COMPLETED,
        nTotalMatches: nTotalMatches,
        cdsFinished: now.toISOString()
    });

    if (!DEBUG) {
        const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(fullSearchInputName);
        await removeKey(searchBucket, intermediateSearchResultsPrefix);
        // TODO: delete items from DynamoDB using BatchWriteItem
    }

    return {
        ...event,
        matchCounts
    };
};
