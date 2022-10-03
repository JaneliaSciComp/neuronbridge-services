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

// Extract results from the database and map to the final result
const extractResults = (item) => {
    const resultsSValue = item.resultsMimeType === 'application/gzip'
        ? zlib.gunzipSync(item.results)
        : item.results;
    const intermediateResult = JSON.parse(resultsSValue);
    return convertItermediateResults(intermediateResult);
};

// Convert intermediate results to final results
const convertItermediateResults = (item) => {
    const inputImage = {
        id: item.maskId,
        libraryName: item.maskLibraryName,
        publishedName: item.maskPublishedName,
        files: {
          ColorDepthMip: item.maskImageURL,
        },
    };
    const results = item && item.results
        ? item.results.map(m => convertMatch(m))
        : [];
    return {
        inputImage: inputImage,
        results: results,
    };
};

const convertMatch = (cdm) => {
    return {
        image: {
            id: cdm.id,
            store: cdm.libraryStore,
            libraryName: cdm.libraryName,
            publishedName: cdm.publishedName,
            alignmentSpace: cdm.alignmentSpace,
            gender: cdm.gender,
            anatomicalArea: cdm.anatomicalArea,
            slideCode: cdm.slideCode,
            objective: cdm.objective,
            channel: cdm.channel,
            files: {
              ColorDepthMip: cdm.imageURL,
              ColorDepthMipThumbnail: cdm.thumbnailURL,
            },
        },
        files: {
            ColorDepthMipInput: cdm.maskImageName,
            ColorDepthMipMatch: cdm.imageName,
        },
        mirrored: cdm.mirrored,
        normalizedScore: cdm.normalizedScore,
        matchingPixels: cdm.matchingPixels,
        matchingRatio: cdm.matchingRatio,
    };
};

// Merge results in the final form
const mergeResults = (rs1, rs2) => {
    if (!rs1.inputImage) {
        throw new Error('Results cannot be merged because rs1.inputImage is not set', rs1, rs2);
    }
    if (!rs2.inputImage) {
        throw new Error('Results cannot be merged because rs2.inputImage is not set', rs1, rs2);
    }
    if (rs1.inputImage.id === rs2.inputImage.id) {
        const allMergedResults = [...rs1.results, ...rs2.results];
        // sort the merged results
        allMergedResults.sort((r1, r2) => r2.matchingPixels - r1.matchingPixels);
        // if merged results is too large, truncate it
        const mergedResults = maxResultsLength > 0 ? allMergedResults.slice(0, maxResultsLength) : allMergedResults;
        return {
            inputImage: rs1.inputImage,
            results: mergedResults,
        };
    } else {
        throw new Error(`Results could not be merged because mask IDs: ${rs1.inputImage.id} and ${rs2.inputImage.id} are different`);
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
