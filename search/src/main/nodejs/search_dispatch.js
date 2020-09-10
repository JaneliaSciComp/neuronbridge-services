'use strict';

const AWS = require('aws-sdk');
const AWSXRay = require('aws-xray-sdk-core')
const { v1: uuidv1 } = require('uuid');

const {getSearchMetadataKey, getIntermediateSearchResultsKey} = require('./searchutils');
const {getObject, putObject, invokeAsync, partition, startStepFunction, verifyKey, DEBUG} = require('./utils');
const {getSearchMetadata, updateSearchMetadata, SEARCH_IN_PROGRESS} = require('./awsappsyncutils');

const DEFAULTS = {
    level: 0,
    numLevels: 2,
    maskThreshold: 100,
    dataThreshold: 100,
    pixColorFluctuation: 2.0,
    xyShift: 2,
    mirrorMask: true,
    minMatchingPixRatio: 2,
};

const defaultBatchSize = () => {
    if (process.env.BATCH_SIZE) {
        const configuredBatchSize = process.env.BATCH_SIZE *  1;
        return configuredBatchSize > 0 ? configuredBatchSize : 40;
    } else {
        return 40;
    }
}

const MAX_PARALLELISM = process.env.MAX_PARALLELISM || 3000;
const region = process.env.AWS_REGION;
const defaultLibraryBucket = process.env.LIBRARY_BUCKET;
const defaultSearchBucket = process.env.SEARCH_BUCKET;
const dispatchFunction = process.env.SEARCH_DISPATCH_FUNCTION;
const searchFunction = process.env.SEARCH_FUNCTION;
const stateMachineArn = process.env.STATE_MACHINE_ARN;

exports.searchDispatch = async (event) => {

    if (DEBUG) console.log(event);

    const segment = AWSXRay.getSegment();
    var subsegment = segment.addNewSubsegment('Read parameters');

    const searchInputParams = await getSearchInputParams(event);

    const searchId = searchInputParams.searchId;
    const searchBucket = searchInputParams.searchBucket || defaultSearchBucket;
    const libraryBucket = searchInputParams.libraryBucket || defaultLibraryBucket;
    const searchType = searchInputParams.searchType;
    const searchInputFolder = searchInputParams.searchInputFolder;
    const searchInputName = searchInputParams.searchInputName;

    // Parameters which have defaults
    const level = searchInputParams.level || DEFAULTS.level;
    const numLevels = searchInputParams.numLevels || DEFAULTS.numLevels;
    const dataThreshold = searchInputParams.dataThreshold || DEFAULTS.dataThreshold;
    const pixColorFluctuation = searchInputParams.pixColorFluctuation || DEFAULTS.pixColorFluctuation;
    const xyShift = searchInputParams.xyShift || DEFAULTS.xyShift;
    const mirrorMask = searchInputParams.mirrorMask || DEFAULTS.mirrorMask;
    const minMatchingPixRatio = searchInputParams.minMatchingPixRatio || DEFAULTS.minMatchingPixRatio;
    const maskThreshold = searchInputParams.maskThreshold || DEFAULTS.maskThreshold

    // Programmatic parameters. In the case of the root manager, these will be null initially and then generated for later invocations.
    let libraries = searchInputParams.libraries;
    let batchSize = searchInputParams.batchSize || defaultBatchSize();
    let numBatches = searchInputParams.numBatches;
    let branchingFactor = searchInputParams.branchingFactor;
    let startIndex = searchInputParams.startIndex;
    let endIndex = searchInputParams.endIndex;

    subsegment.close();

    if (level === 0) {
        const maskKey = `${searchInputFolder}/${searchInputName}`;
        const checkMask = await verifyKey(searchBucket, maskKey);
        if (checkMask === false) {
            console.log(`Mask s3://${searchBucket}/${maskKey} not found`);
            // set the error
            await updateSearchMetadata({
                id: searchId,
                step: SEARCH_IN_PROGRESS,
                errorMessage: `Mask s3://${searchBucket}/${maskKey} not found`
            });
            return searchInputName;
        }
        subsegment = segment.addNewSubsegment('Prepare batch parallelization parameters');
        const searchInputParamsWithLibraries = setSearchLibraries(searchInputParams);
        console.log("Search input params with libraries", searchInputParamsWithLibraries);
        const librariesPromises = await searchInputParamsWithLibraries.libraries
            .map(lname => {
                // for searching use MIPs from the searchableMIPs folder
                const libraryFolder = searchInputParamsWithLibraries.searchableMIPSFolder
                    ? `${lname}/${searchInputParamsWithLibraries.searchableMIPSFolder}`
                    : lname;
                const libraryAlignmentSpace = searchInputParamsWithLibraries.libraryAlignmentSpace
                    ? `${searchInputParamsWithLibraries.libraryAlignmentSpace}/`
                    : ''
                const library = {
                    lname: lname,
                    lkey: `${libraryAlignmentSpace}${libraryFolder}`
                };
                console.log("Lookup library", library);
                return library;
            })
            .map(async l => {
                const lsize = await getCount(libraryBucket, l.lkey);
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
        if (totalSearches === 0) {
            console.log(`No libraries found for searching ${searchInputName}`);
            // set the error
            await updateSearchMetadata({
                id: searchId,
                step: SEARCH_IN_PROGRESS,
                errorMessage: `No libraries found for searching ${searchInputName}`
            });
            return searchInputName;
        }
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
        subsegment.close();

        const now = new Date();
        const searchMetadata = {
            startTime: now.toISOString(),
            searchType: searchInputParamsWithLibraries.searchType,
            parameters: searchInputParamsWithLibraries,
            libraries: libraries,
            nsearches: totalSearches,
            branchingFactor: branchingFactor,
            partitions: numBatches
        };
        // persist the search metadata on S3
        await putObject(
            searchBucket,
            getSearchMetadataKey(`${searchInputParamsWithLibraries.searchInputFolder}/${searchInputParamsWithLibraries.searchInputName}`),
            searchMetadata);
        // update search metadata if searchId is provided
        await updateSearchMetadata({
            id: searchId,
            step: SEARCH_IN_PROGRESS,
            nBatches: numBatches,
            completedBatches: 0,
            cdsStarted: now.toISOString()
        });
        if (stateMachineArn != null) {
            // if monitoring then start it right away
            const monitorParams = {
                bucket: searchBucket,
                searchId: searchId || null,
                searchInputFolder,
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
        searchBucket: searchBucket,
        libraryBucket: libraryBucket,
        libraries: libraries,
        searchId: searchId,
        searchType: searchType,
        searchInputFolder: searchInputFolder,
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
                    startIndex: workerStart,
                    endIndex: workerEnd,
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
                    lkeys: await getKeys(libraryBucket, l.lkey)
                };
            });
        const librariesWithKeys = await Promise.all(librariesWithKeysPromise);
        const allKeys = librariesWithKeys
            .map(l => l.lkeys)
            .reduce((acc, lkeys) => acc.concat(lkeys), []);
        const keys = allKeys.slice(startIndex, endIndex);
        const batchPartitions = partition(keys, batchSize);
        let batchIndex = Math.ceil(startIndex / batchSize);
        console.log(`Selected keys from ${startIndex} to ${endIndex} out of ${allKeys.length} keys for ${batchPartitions.length} batches starting with ${batchIndex} from`,
            libraries);
        subsegment.close();

        subsegment = segment.addNewSubsegment('Invoke batches');
        for (const searchKeys of batchPartitions) {
            const batchResultsKey = getIntermediateSearchResultsKey(`${searchInputFolder}/${searchInputName}`, batchIndex);
            const batchResultsURI = `s3://${searchBucket}/${batchResultsKey}`;
            const searchParams = {
                searchId: searchId,
                outputURI: batchResultsURI,
                maskPrefix: searchBucket,
                maskKeys: [`${searchInputFolder}/${searchInputName}`],
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

const getSearchInputParams = async (event) => {
    const searchId = event.searchId;
    let searchMetadata;
    if (!event.searchInputName || !event.searchInputFolder) {
        // if searchInputName or searchInutFolder is not given the searchId must be provided
        // both searchInputFolder and searchInputName must be provided because
        // the full input path is `${searchInputFolder}/${searchInputName}`
        if (!searchId) {
            throw new Error('Missing required parameter: "searchId"');
        }
        searchMetadata = await getSearchMetadata(searchId);
    } else {
        searchMetadata = event;
    }
    if (searchMetadata) {
        // if a searchMask is set use that for search otherwise use the upload
        searchMetadata.searchInputName = searchMetadata.searchMask
            ? searchMetadata.searchMask
            : searchMetadata.searchInputName;
    }
    console.log("Searching params", searchMetadata);
    return searchMetadata;
}

const setSearchLibraries = (searchData)  => {
    switch (searchData.searchType) {
        case 'em2lm':
            return {
                ...searchData,
                libraryAlignmentSpace: 'JRC2018_Unisex_20x_HR',
                searchableMIPSFolder: 'searchable_neurons',
                libraries: [
                    'FlyLight_Split-GAL4_Drivers',
                    'FlyLight_Gen1_MCFO'
                ]
            };
        case 'lm2em':
            return {
                ...searchData,
                libraryAlignmentSpace: 'JRC2018_Unisex_20x_HR',
                searchableMIPSFolder: 'searchable_neurons',
                libraries: [
                    "FlyEM_Hemibrain_v1.1"
                ]
            };
        default:
            return {
                ...searchData,
                libraries: searchData.libraries || []
            };
    }
}

const getCount = async (libraryBucket, libraryKey) => {
    console.log("Get count from:", libraryKey);
    const countMetadata = await getObject(
        libraryBucket,
        `${libraryKey}/counts_denormalized.json`,
        {objectCount: 0}
    );
    return countMetadata.objectCount;
}

const getKeys = async (libraryBucket, libraryKey) => {
    console.log("Get keys from:", libraryKey);
    return await getObject(
        libraryBucket,
        `${libraryKey}/keys_denormalized.json`,
        []);
}

const startMonitor = async (searchId, monitorParams, stateMachineArn, segment) => {
    let subsegment = segment.addNewSubsegment('Start monitor');
    const now = new Date().getTime();
    const uniqueMonitorId = searchId || uuidv1();
    await startStepFunction(
        `ColorDepthSearch_${uniqueMonitorId}_${now}`,
        monitorParams,
        stateMachineArn
    );
    subsegment.close();
}
