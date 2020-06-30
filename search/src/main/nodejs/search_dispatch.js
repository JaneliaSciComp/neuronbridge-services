'use strict';

const AWS = require('aws-sdk');
const AWSXRay = require('aws-xray-sdk-core')
const { v1: uuidv1 } = require('uuid');

const {getSearchMetadataKey, getSearchParamsKey, getSearchProgressKey, getIntermediateSearchResultsKey} = require('./searchutils');
const {getObject, putObject, putText, invokeAsync, partition, DEBUG} = require('./utils');

const DEFAULTS = {
    level: 0,
    numLevels: 2,
    batchSize: 40,
    maskThreshold: 100,
    dataThreshold: 100,
    pixColorFluctuation: 2.0,
    xyShift: 0,
    mirrorMask: false,
    minMatchingPixRatio: 2,
};

const MAX_PARALLELISM = process.env.MAX_PARALLELISM || 1000;
const region = process.env.AWS_REGION;
const libraryBucket = process.env.LIBRARY_BUCKET;
const searchBucket = process.env.SEARCH_BUCKET;
const dispatchFunction = process.env.DISPATCH_FUNCTION;
const searchFunction = process.env.SEARCH_FUNCTION;
const stateMachineArn = process.env.STATE_MACHINE_ARN;

exports.searchDispatch = async (event) => {

    if (DEBUG) console.log(event);

    const segment = AWSXRay.getSegment();
    var subsegment = segment.addNewSubsegment('Read parameters');

    const searchInputName = event.searchInputName;

    if (!searchInputName) {
        throw new Error('Missing required parameter: "searchInputName"');
    }

    // Parameters which have defaults
    const level = event.level || DEFAULTS.level;
    const numLevels = event.numLevels || DEFAULTS.numLevels;
    const dataThreshold = event.dataThreshold || DEFAULTS.dataThreshold;
    const pixColorFluctuation = event.pixColorFluctuation || DEFAULTS.pixColorFluctuation;
    const xyShift = event.xyShift || DEFAULTS.xyShift;
    const mirrorMask = event.mirrorMask || DEFAULTS.mirrorMask;
    const minMatchingPixRatio = event.minMatchingPixRatio || DEFAULTS.minMatchingPixRatio;

    // Programmatic parameters. In the case of the root manager, these will be null initially and then generated for later invocations.
    const searchId = event.searchId || uuidv1();
    let maskThreshold = event.maskThreshold;
    let libraries = event.libraries;
    let batchSize = event.batchSize || DEFAULTS.batchSize;
    let numBatches = event.numBatches;
    let branchingFactor = event.branchingFactor;
    let startIndex = event.startIndex;
    let endIndex = event.endIndex;

    subsegment.close();

    if (level === 0) {
        subsegment = segment.addNewSubsegment('Create metadata');
        const searchInputParams = await getSearchInputParams(searchInputName);
        maskThreshold = searchInputParams.maskThreshold || DEFAULTS.maskThreshold
        if (!searchInputParams.libraries) {
            throw new Error(`Missing libraries for ${searchInputName}`);
        }
        console.log("Searching params", searchInputParams);
        const librariesPromises = await searchInputParams.libraries
            .map(lname => {
                return {
                    lname: lname,
                    lkey: `JRC2018_Unisex_20x_HR/${lname}`
                };
            })
            .map(async l => {
                const lsize = await getCount(l.lkey);
                return await {
                    ...l,
                    lsize: lsize
                };
            });
        libraries =  await Promise.all(librariesPromises);
        console.log(`Input MIP libraries: `, libraries);
        const totalSearches = libraries
            .map(l => l.lsize)
            .reduce((acc, lsize) => acc + lsize, 0);
        console.log(`Found ${totalSearches} MIPs in libraries: `, libraries);
        numBatches = Math.ceil(totalSearches / batchSize);
        console.log(`Partition ${totalSearches} searches into ${numBatches} of size ${batchSize}`);
        if (numBatches > MAX_PARALLELISM) {
            // adjust the batch size
            batchSize = Math.ceil(totalSearches / MAX_PARALLELISM)
            numBatches = Math.ceil(totalSearches / batchSize);
            console.log(`Capping batch size to ${batchSize} due to max parallelism (${MAX_PARALLELISM})`);
        }
        // the branchingFactor formula assumes that each leaf node at level = <numLevels> corresponds to a batch of size <batchSize>
        // nLeafNodes = totalSearches / batchSize = branchingFactor ^ numLevels
        branchingFactor = Math.ceil(Math.pow(numBatches, 1/numLevels)); // e.g. ceil(695^(1/3)) = ceil(8.86) = 9
        startIndex = 0
        endIndex = totalSearches;
        const now = new Date();
        const searchMetadata = {
            startTime: now.toISOString(),
            parameters: event,
            libraries: libraries,
            nsearches: totalSearches,
            branchingFactor: branchingFactor,
            partitions: numBatches
        };

        // persist the search metadata and the progress file
        await putObject(searchBucket, getSearchMetadataKey(searchInputName), searchMetadata);
        await putText(searchBucket, getSearchProgressKey(searchInputName), "0");

        subsegment.close();

        if (stateMachineArn != null) {
            // if monitoring then start it right away
            const monitorParams = {
                searchId,
                bucket: searchBucket,
                searchInputName,
                startTime: now.toISOString(),
                numBatches,
            }
            await startMonitor(searchId, monitorParams, stateMachineArn, segment);
        }

    }
    const nextLevelManagerRange = Math.pow(branchingFactor, numLevels-level-1) * batchSize;
    console.log(`Level ${level} -> next range: ${nextLevelManagerRange}`);
    const nextEvent = {
        level: level + 1,
        numLevels: numLevels,
        libraries: libraries,
        searchId: searchId,
        searchInputName: searchInputName,
        dataThreshold: dataThreshold,
        maskThreshold: maskThreshold,
        pixColorFluctuation: pixColorFluctuation,
        xyShift: xyShift,
        mirrorMask: mirrorMask,
        minMatchingPixRatio: minMatchingPixRatio,
        batchSize: batchSize,
        numBatches: numBatches,
        branchingFactor: branchingFactor
    }

    if (level + 1 < numLevels) {
        subsegment = segment.addNewSubsegment('Start intermediate level dispatchers');
        // start more intermediate dispatchers
        for(let i = startIndex; i < endIndex; i += nextLevelManagerRange) {
            const workerStart = i;
            const workerEnd = i+nextLevelManagerRange > endIndex ? endIndex : i+nextLevelManagerRange;
            await invokeAsync(
                dispatchFunction, {
                    startIndex:workerStart,
                    endIndex:workerEnd,
                    ...nextEvent
                });
            console.log(`Dispatched sub-manager ${workerStart} - ${workerEnd}`);
        }
        subsegment.close();
    } else {
        // this is the parent of leaf node (each leaf node corresponds to a batch) so start the batch
        subsegment = segment.addNewSubsegment('Get library keys');
        const librariesWithKeysPromise =  await libraries
            .map(async l => {
                return await {
                    ...l,
                    lkeys: await getKeys(l.lkey)
                };
            });
        const librariesWithKeys = await Promise.all(librariesWithKeysPromise);
        const allKeys = librariesWithKeys
            .map(l => l.lkeys)
            .reduce((acc, lkeys) => acc.concat(lkeys), []);
        const keys = allKeys.slice(startIndex, endIndex);
        console.log(`Selected keys from ${startIndex} to ${endIndex} out of ${allKeys.length} keys from`, libraries);
        const batchPartitions = partition(keys, batchSize);
        subsegment.close();

        subsegment = segment.addNewSubsegment("Get search params");
        subsegment.close();

        subsegment = segment.addNewSubsegment('Invoke batches');
        let batchIndex = Math.ceil(startIndex, batchSize);
        for (const searchKeys of batchPartitions) {
            const batchResultsKey = getIntermediateSearchResultsKey(searchInputName, batchIndex);
            const batchResultsURI = `s3://${searchBucket}/${batchResultsKey}`;
            const searchParams = {
                outputURI: batchResultsURI,
                maskPrefix: searchBucket,
                maskKeys: [searchInputName],
                maskThresholds: [maskThreshold],
                searchPrefix: libraryBucket,
                searchKeys,
                ...nextEvent
            };
            await invokeAsync(searchFunction, searchParams);
            console.log(`Dispatched batch #${batchIndex} (${searchInputName} with ${searchKeys.length} items)`);
            batchIndex++;
        }
        subsegment.close();
    }

    return searchInputName;
}

const getSearchInputParams = async (searchInputName)  => {
    const searchInput = await getObject(searchBucket, getSearchParamsKey(searchInputName));
    switch (searchInput.searchtype) {
        case "em2lm":
            return {
                ...searchInput,
                libraries: [
                    "FlyLight_Split-GAL4_Drivers"
                ]
            };
        case "lm2em":
            return {
                ...searchInput,
                libraries: [
                    "FlyEM_Hemibrain_v1.0"
                ]
            };
        default:
            return {
                ...searchInput,
                libraries: []
            };
    }
}

const getCount = async (libraryKey) => {
    const countMetadata = await getObject(libraryBucket, `${libraryKey}/counts_denormalized.json`);
    console.log("Retrieved count metadata: ", countMetadata);
    return countMetadata.objectCount;
}

const getKeys = async (libraryKey) => {
    return await getObject(libraryBucket, `${libraryKey}/keys_denormalized.json`);
}

const startMonitor = async (searchId, monitorParams, stateMachineArn, segment) => {
    let subsegment = segment.addNewSubsegment('Start monitor');
    const stepFunction = new AWS.StepFunctions();
    const params = {
        stateMachineArn: stateMachineArn,
        input: JSON.stringify(monitorParams),
        name: `ColorDepthSearch_${searchId}`
    };
    const result = await stepFunction.startExecution(params).promise();

    console.log("Step function started: ", result.executionArn);
    subsegment.close();
}
