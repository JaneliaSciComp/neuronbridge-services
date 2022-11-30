import { getIntermediateSearchResultsPrefix, getSearchMaskId, getSearchResultsKey } from './searchutils';
import { streamObject, removeKey, DEBUG } from './utils';
import { queryDb } from './clientDbUtils';
import { updateSearchMetadata, SEARCH_COMPLETED } from './awsappsyncutils';
import zlib from 'zlib';

const maxResultsLength = process.env.MAX_CUSTOM_RESULTS || -1;

const mergeBatchResults = async (searchId, items, allBatchResults) => {
    let nMergedResults = 0;
    for (const item of items) {
        try {
            const batchResults = extractResults(item);
            batchResults.forEach(batchResult => {
                // batchResult looks like:
                // {
                //     inputImage: {
                //         filename:
                //         libraryName:
                //     },
                //     results: [
                //         image: {
                //             id:
                //         },
                //         files: {
                //             CDMInput:
                //             CDMMatch:
                //         }
                //     ]
                // }
                if (allBatchResults[batchResult.inputImage.filename]) {
                    // assign merged results to the current mask
                    allBatchResults[batchResult.inputImage.filename] = mergeResults(allBatchResults[batchResult.inputImage.filename], batchResult);
                } else {
                    // first results for the current input mask
                    allBatchResults[batchResult.inputImage.filename] = batchResult;
                }
            });
            nMergedResults += batchResults.length;
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
    console.log(`Merged ${nMergedResults} color depth search results for ${searchId}`);
};

// Extract results from the database and map to the final result
const extractResults = (item) => {
    try {
        const resultsSValue = item.resultsMimeType === 'application/gzip'
            ? zlib.gunzipSync(item.results)
            : item.results;
        const intermediateResults = JSON.parse(resultsSValue);
        // convert all intermediate results
        return intermediateResults.map(r => convertItermediateResults(r));
    } catch (e) {
        console.log(`Error extracting results for ${item.jobId}:${item.batchId}`);
        throw e;
    }
};

// Convert intermediate results to final results
const convertItermediateResults = item => {
    const maskImagePath = getMaskImagePathFromURL(item.maskImageURL);
    // for the inputImage the store is actually the search bucket itself
    const inputImage = {
        filename: item.maskId,
        libraryName: item.maskLibraryName,
        publishedName: item.maskPublishedName,
        files: {
            store: '',
            CDM: maskImagePath,
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

const getMaskImagePathFromURL = maskURLValue => {
    const maskURL = new URL(maskURLValue);
    // maskURL.pathname includes the bucket name and it looks like:
    // '/bucket/private/userid/searchfolder/generatedMIPs/file.png'
    // so the first components of the split invocation are:
    // ['', 'bucket', 'private', 'userid', 'searchfolder', ...]
    // we only want to keep components after the search folder (including the search folder)
    return pathRelativeToNComp(maskURL.pathname, 4);
};

const pathRelativeToNComp = (aPath, startSubpathComp) => {
    const pComps = aPath.split('/');
    return pComps.slice(startSubpathComp).join('/');
};

const convertMatch = (cdm) => {
    // the initial maskImageName: "private/userid/searchfolder/..."
    // what I want is the name relative to "private/userid", i.e., "searchfolder/..."
    const maskImageName = pathRelativeToNComp(cdm.maskImageName, 2);
    const matchedImageName = getDisplayableImage(cdm.imageName);
    const publishedName = cdm.publishedNamePrefix
                            ? `${cdm.publishedNamePrefix}:${cdm.publishedName}`
                            : cdm.publishedName;
    const anatomicalArea = cdm.anatomicalArea.toLowerCase() === 'vnc'
        ? 'VNC'
        : 'Brain';
    return {
        image: {
            id: cdm.id,
            libraryName: cdm.libraryName,
            publishedName: publishedName,
            alignmentSpace: cdm.alignmentSpace,
            gender: cdm.gender,
            anatomicalArea,
            slideCode: cdm.slideCode,
            objective: cdm.objective,
            channel: cdm.channel,
            type: cdm.targetType,
            files: {
                store: cdm.libraryStore,
                CDM: cdm.imageURL,
                CDMThumbnail: cdm.thumbnailURL,
            },
        },
        files: {
            store: cdm.libraryStore,
            CDMInput: maskImageName,
            CDMMatch: matchedImageName,
        },
        mirrored: cdm.mirrored,
        normalizedScore: cdm.normalizedScore,
        matchingPixels: cdm.matchingPixels,
        matchingRatio: cdm.matchingRatio,
        type: "CDSMatch",
    };
};

const getDisplayableImage = (fullImageName) => {
    if (fullImageName.endsWith('.tif') || fullImageName.endWith('.tiff')) {
        // we assume this is a segmentation image located in a certain partition like:
        // <as>/<library>/searchable_neurons/<partition>/<name>.tif
        // to get the displayable image we replace <partition> with "pngs" and
        // the ".tif" extension with ".png"
        const imageNameComps = fullImageName.split('/');
        const imageName = imageNameComps[imageNameComps.length - 1];
        // replace partition folder with 'pngs' folder
        imageNameComps[imageNameComps.length - 2] = 'pngs';
        // replace .tif extension with .png
        imageNameComps[imageNameComps.length - 1] = imageName.replace(/\.tiff?$/, '.png');
        return imageNameComps.join('/');
    } else {
        // don't know how to handle this
        return fullImageName;
    }
};

// Merge results in the final form
const mergeResults = (rs1, rs2) => {
    if (!rs1.inputImage) {
        throw new Error('Results cannot be merged because rs1.inputImage is not set', rs1, rs2);
    }
    if (!rs2.inputImage) {
        throw new Error('Results cannot be merged because rs2.inputImage is not set', rs1, rs2);
    }
    if (rs1.inputImage.filename === rs2.inputImage.filename) {
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
        throw new Error(`Results could not be merged because mask IDs: ${rs1.inputImage.filename} and ${rs2.inputImage.filename} are different`);
    }
};

const updateResults3DFiles = async (resultsList) => {
    console.log(`Update 3D files for ${resultsList.length} search results`);
    const lmPublishedStacksTable = process.env.LM_PUBLISHED_STACKS_TABLE;
    const emPublishedSkeletonsTable = process.env.EM_PUBLISHED_SKELETONS_TABLE;
    const updatedResultsPromises = resultsList.map(async r => {
        const matchedImage = r.image;
        if (matchedImage.type === 'LMImage' && lmPublishedStacksTable) {
            const alignmentSpace = matchedImage.alignmentSpace;
            const slideCode = matchedImage.slideCode;
            const objective = matchedImage.objective;
            const key = `${slideCode}-${objective}-${alignmentSpace}`.toLowerCase();
            const queryParams = {
                TableName: lmPublishedStacksTable,
                ConsistentRead: true,
                KeyConditionExpression: 'itemType = :itemType',
                ExpressionAttributeValues: {
                    ':itemType': key,
                },
            };
            const publishedImageItems = await queryDb(queryParams);
            if (publishedImageItems && publishedImageItems.Items && publishedImageItems.Items.length > 0) {
                const publishedImage = publishedImageItems.Items[0];
                if (publishedImage.files) {
                    r.image.files.VisuallyLosslessStack = relativePathFromURL(publishedImage.files.VisuallyLosslessStack);
                }
            }
        } else if (matchedImage.type == 'EMImage' && emPublishedSkeletonsTable) {
            const publishedName = matchedImage.publishedName;
            const queryParams = {
                TableName: emPublishedSkeletonsTable,
                ConsistentRead: true,
                KeyConditionExpression: 'publishedName = :publishedName',
                ExpressionAttributeValues: {
                    ':publishedName': publishedName,
                },
            };
            const publishedImageItems = await queryDb(queryParams);
            if (publishedImageItems && publishedImageItems.Items && publishedImageItems.Items.length > 0) {
                const publishedImage = publishedImageItems.Items[0];
                r.image.files.AlignedBodyOBJ = relativePathFromURL(publishedImage.skeletonobj);
                r.image.files.AlignedBodySWC = relativePathFromURL(publishedImage.skeletonswc);
            }
        }
        return await r;
    });
    return await Promise.all(updatedResultsPromises);
};

const relativePathFromURL = (aURL) => {
    if (!aURL) {
        return ''; // aURL is nullable
    }
    try {
        let protocol;
        let startPath;
        if (aURL.startsWith('https://')) {
            // the URL is: https://<awsdomain>/<bucket>/<prefix>/<fname>
            protocol = 'https://';
            startPath = 2;
        } else if (aURL.startsWith('http://')) {
            // the URL is: http://<awsdomain>/<bucket>/<prefix>/<fname>
            protocol = 'http://';
            startPath = 2;
        } else if (aURL.startsWith('s3://')) {
            // the URL is: s3://<bucket>/<prefix>/<fname>
            protocol = 's3://';
            startPath = 1;
        } else {
            // the protocol either is not set or unsupported:
            console.log(`Protocol not set or unsupported in ${aURL} - returning the value as is`);
            return aURL;
        }
        return pathRelativeToNComp(aURL.substring(protocol.length), startPath);
    } catch (e) {
        console.error(`Erroor getting relative path for ${aURL}`, e);
        return aURL;
    }
};

export const searchCombiner = async (event) => {
    if (DEBUG) console.log('Input event:', JSON.stringify(event));

    // Parameters
    const { jobId, tasksTableName, timedOut, completed, withErrors, fatalErrors } = event;
    const { searchBucket, searchId, maskKeys, maxResultsPerMask } = event.jobParameters;
    const fullSearchInputName = maskKeys[0];
    const searchInputName = fullSearchInputName.substring(fullSearchInputName.lastIndexOf("/") + 1);

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
        queryResult = await queryDb(params);
        console.log(`Merging ${queryResult.Items.length} results`, '->', queryResult.LastEvaluatedKey ? queryResult.LastEvaluatedKey : 'end');
        mergeBatchResults(searchId, queryResult.Items, allBatchResults);
        params.ExclusiveStartKey = queryResult.LastEvaluatedKey;
    } while (queryResult.LastEvaluatedKey);

    const nTotalMatches = Object.values(allBatchResults)
        .map(rsByMask => {
            return rsByMask.results.length;
        })
        .reduce((a, n) => a + n, 0);

    console.log(`Total number of matches for ${jobId} is ${nTotalMatches}`, allBatchResults);

    const allMatchesPromises = Object.values(allBatchResults)
        .map(async rsByMask => {
            const results = rsByMask.results;
            console.log(`Sort ${results.length} for ${rsByMask.inputImage.filename}`);
            results.sort((r1, r2) => r2.matchingPixels - r1.matchingPixels);
            if (maxResultsPerMask && maxResultsPerMask > 0 && results.length > maxResultsPerMask) {
                rsByMask.results = results.slice(0, maxResultsPerMask);
            }
            rsByMask.results = await updateResults3DFiles(rsByMask.results);
            return rsByMask;
        });

    const allMatches = await Promise.all(allMatchesPromises);

    // write down the results
    console.log(`Save color depth search results for ${fullSearchInputName}`);
    const searchResultsKey = getSearchResultsKey(fullSearchInputName);
    console.log(`Write results for ${jobId} to ${searchResultsKey}`);
    const outputUri = await streamObject(
        searchBucket,
        searchResultsKey,
        allMatches.length > 1
            ? allMatches
            : (allMatches[0]
                ? allMatches[0]
                : {
                    inputImage: {
                        filename: getSearchMaskId(searchInputName),
                        files: {
                            store: '',
                            CDM: pathRelativeToNComp(fullSearchInputName, 2),
                        },
                    },
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
        nTotalMatches
    };
};
