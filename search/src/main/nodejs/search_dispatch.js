'use strict';

const { v1: uuidv1 } = require('uuid');

const {getSearchMetadataKey, getIntermediateSearchResultsKey, getSearchMaskId} = require('./searchutils');
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
    negativeRadius: 10,
    withGradientScore: true,
    maxResultsPerMask: -1,
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
const defaultLibraryBucket = process.env.LIBRARY_BUCKET;
const defaultSearchBucket = process.env.SEARCH_BUCKET;
const maxHemibrainMask = process.env.MAX_HEMIBRAIN_MASK_NAME;
const dispatchFunction = process.env.SEARCH_DISPATCH_FUNCTION;
const searchFunction = process.env.SEARCH_FUNCTION;
const stateMachineArn = process.env.STATE_MACHINE_ARN;

exports.searchDispatch = async (event) => {

    // This next log statement is parsed by the analyzer. DO NOT CHANGE.
    console.log('Input event:', JSON.stringify(event));

    const searchInputParams = await getSearchInputParams(event);
    if (DEBUG) console.log('Input params:', searchInputParams);

    const searchId = searchInputParams.searchId;
    const searchBucket = searchInputParams.searchBucket || defaultSearchBucket;
    const libraryBucket = searchInputParams.libraryBucket || defaultLibraryBucket;
    const searchType = searchInputParams.searchType;
    const searchInputFolder = searchInputParams.searchInputFolder;
    const searchInputName = searchInputParams.searchInputName;

    // Parameters which have defaults
    const level = parseInt(searchInputParams.level) || DEFAULTS.level;
    const numLevels = parseInt(searchInputParams.numLevels) || DEFAULTS.numLevels;
    const dataThreshold = parseInt(searchInputParams.dataThreshold) || DEFAULTS.dataThreshold;
    const pixColorFluctuation = parseFloat(searchInputParams.pixColorFluctuation) || DEFAULTS.pixColorFluctuation;
    const xyShift = parseInt(searchInputParams.xyShift) || DEFAULTS.xyShift;
    const mirrorMask = searchInputParams.mirrorMask || DEFAULTS.mirrorMask;
    const minMatchingPixRatio = searchInputParams.minMatchingPixRatio || DEFAULTS.minMatchingPixRatio;
    const maskThreshold = searchInputParams.maskThreshold || DEFAULTS.maskThreshold;
    const negativeRadius = searchInputParams.negativeRadius || DEFAULTS.negativeRadius;
    const withGradientScore = searchInputParams.withGradientScore || DEFAULTS.withGradientScore;
    const maxResultsPerMask =  searchInputParams.maxResultsPerMask || DEFAULTS.maxResultsPerMask;

    // Programmatic parameters. In the case of the root manager, these will be null initially and then generated for later invocations.
    let libraries = searchInputParams.libraries;
    let monitorName = searchInputParams.monitorName;
    let batchSize = parseInt(searchInputParams.batchSize) || defaultBatchSize();
    let numBatches = parseInt(searchInputParams.numBatches);
    let branchingFactor = parseInt(searchInputParams.branchingFactor);
    let startIndex = parseInt(searchInputParams.startIndex);
    let endIndex = parseInt(searchInputParams.endIndex);
    let response = {};

    if (level === 0) {
        // This next log statement is parsed by the analyzer. DO NOT CHANGE.
        console.log("Root Dispatcher");

        const maskKey = `${searchInputFolder}/${searchInputName}`;
        const checkMask = await verifyKey(searchBucket, maskKey);
        if (checkMask === false) {
            const errMsg = `Mask s3://${searchBucket}/${maskKey} not found`;
            // set the error
            await updateSearchMetadata({
                id: searchId,
                step: SEARCH_IN_PROGRESS,
                errorMessage: errMsg
            });
            throw new Error(errMsg);
        }
        const searchInputParamsWithLibraries = setSearchLibraries(searchInputParams);
        console.log("Search input params with libraries", searchInputParamsWithLibraries);
        const librariesPromises = await searchInputParamsWithLibraries.libraries
            .map(lname => {
                // for searching use MIPs from the searchableMIPs folder
                const libraryAlignmentSpace = searchInputParamsWithLibraries.libraryAlignmentSpace
                    ? `${searchInputParamsWithLibraries.libraryAlignmentSpace}/`
                    : ''
                const searchableMIPSFolder = searchInputParamsWithLibraries.searchableMIPSFolder
                    ? `${libraryAlignmentSpace}${lname}/${searchInputParamsWithLibraries.searchableMIPSFolder}`
                    : `${libraryAlignmentSpace}${lname}`;
                const gradientsFolder = searchInputParamsWithLibraries.gradientsFolder
                    ? `${libraryAlignmentSpace}${lname}/${searchInputParamsWithLibraries.gradientsFolder}`
                    : null;
                const zgapMasksFolder = searchInputParamsWithLibraries.zgapMasksFolder
                    ? `${libraryAlignmentSpace}${lname}/${searchInputParamsWithLibraries.zgapMasksFolder}`
                    : null;
                const library = {
                    lname: lname,
                    lkey: searchableMIPSFolder,
                    gradientsFolder: gradientsFolder,
                    zgapMasksFolder: zgapMasksFolder
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
            const errMsg = `No libraries found for searching ${searchInputName}`;
            // set the error
            await updateSearchMetadata({
                id: searchId,
                step: SEARCH_IN_PROGRESS,
                errorMessage: errMsg
            });
            throw new Error(errMsg);
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
        const searchMetadataKey = getSearchMetadataKey(`${searchInputParamsWithLibraries.searchInputFolder}/${searchInputParamsWithLibraries.searchInputName}`)
        await putObject(
            searchBucket,
            searchMetadataKey,
            searchMetadata);
        response.searchResultUri = `s3://${searchBucket}/${searchMetadataKey}`
        // update search metadata if searchId is provided
        await updateSearchMetadata({
            id: searchId,
            step: SEARCH_IN_PROGRESS,
            maskThreshold: maskThreshold,
            dataThreshold: dataThreshold,
            pixColorFluctuation: pixColorFluctuation,
            xyShift: xyShift,
            mirrorMask: mirrorMask,
            minMatchingPixRatio: minMatchingPixRatio,
            negativeRadius: negativeRadius,
            withGradientScore: withGradientScore,
            maxResultsPerMask: maxResultsPerMask,
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
                numBatches
            }
            monitorName = await startMonitor(searchId, monitorParams, stateMachineArn);
            response.monitorUniqueName = monitorName       
        }

    }

    if (monitorName) {
        // This next log statement is parsed by the analyzer. DO NOT CHANGE.
        console.log(`Monitor: ${monitorName}`);
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
        negativeRadius: negativeRadius,
        withGradientScore:  withGradientScore,
        maxResultsPerMask: maxResultsPerMask,
        batchSize: batchSize,
        numBatches: numBatches,
        branchingFactor: branchingFactor,
        monitorName: monitorName
    }

    if (level + 1 < numLevels) {
        // start more intermediate dispatchers
        for(let i = startIndex; i < endIndex; i += nextLevelManagerRange) {
            const workerStart = i;
            const workerEnd = i+nextLevelManagerRange > endIndex ? endIndex : i+nextLevelManagerRange;
            const invokeResponse = await invokeAsync(
                dispatchFunction, {
                    startIndex: workerStart,
                    endIndex: workerEnd,
                    ...nextEvent
                });
            console.log(`Dispatched sub-manager ${workerStart} - ${workerEnd} [status=${invokeResponse.Status}]`);
        }
    } else {
        // this is the parent of leaf node (each leaf node corresponds to a batch) so start the batch
        const searchableTargetsPromise =  await libraries
            .map(async l => {
                return await {
                    ...l,
                    searchableKeys: await getKeys(libraryBucket, l.lkey)
                };
            });
        const searchableTargets = await Promise.all(searchableTargetsPromise);
        const allTargets = searchableTargets
            .flatMap(l => l.searchableKeys.map(key => {
                const searchName = getSearchMaskId(key);
                const gradientKey = l.gradientsFolder ? `${l.gradientsFolder}/${searchName}` : null;
                const zgapMaskKey = l.zgapMasksFolder ? `${l.zgapMasksFolder}/${searchName}` : null;
                return {
                    lname: l.lname,
                    searchKey: key,
                    gradientKey: gradientKey,
                    zgapMaskKey: zgapMaskKey,
                    maskROIKey: l.maskROIKey
                };
            }));
        const targets = allTargets.slice(startIndex, endIndex);
        const batchPartitions = partition(targets, batchSize);
        let batchIndex = Math.ceil(startIndex / batchSize);
        console.log(`Selected targets from ${startIndex} to ${endIndex} out of ${allTargets.length} keys for ${batchPartitions.length} batches starting with ${batchIndex} from`,
            libraries);
            
        for (const searchBatch of batchPartitions) {
            // This next log statement is parsed by the analyzer. DO NOT CHANGE.
            console.log(`Dispatching Batch Id: ${batchIndex}`)
            const batchResultsKey = getIntermediateSearchResultsKey(`${searchInputFolder}/${searchInputName}`, batchIndex);
            const batchResultsURI = `s3://${searchBucket}/${batchResultsKey}`;
            const maskROIKey = searchBatch[0].maskROIKey;
            const searchParams = {
                monitorName: monitorName,
                batchId: batchIndex,
                searchId: searchId,
                outputURI: batchResultsURI,
                maskPrefix: searchBucket,
                maskKeys: [`${searchInputFolder}/${searchInputName}`],
                maskThresholds: [maskThreshold],
                searchPrefix: libraryBucket,
                searchKeys: searchBatch.map(st => st.searchKey),
                gradientKeys: searchBatch.filter(st => !!st.gradientKey).map(st => st.gradientKey),
                zgapMaskKeys: searchBatch.filter(st => !!st.zgapMaskKey).map(st => st.zgapMaskKey),
                maskROIKey: maskROIKey,
                ...nextEvent
            };
            const invokeResponse = await invokeAsync(searchFunction, searchParams);
            console.log(`Dispatched batch #${batchIndex} (${searchInputName} with ${searchBatch.length} items) [status=${invokeResponse.Status}]`);
            batchIndex++;
        }
    }

    return response;
}

const getSearchInputParams = async (event) => {
    let searchMetadata;
    // Both searchInputFolder and searchInputName must be provided because
    // the full input path is `${searchInputFolder}/${searchInputName}`
    if (!event.searchInputName || !event.searchInputFolder) {
        // If searchInputName or searchInputFolder is not given the searchId must be provided
        // so that the searchInput path can be retrieved from the database.
        const searchId = event.searchId;
        if (!searchId) {
            throw new Error('Missing required parameter: "searchId"');
        }
        searchMetadata = await getSearchMetadata(searchId);
    } else {
        searchMetadata = event;
    }
    if (!!searchMetadata && !!searchMetadata.searchMask) {
        // if a searchMask is set use that for search otherwise use the upload
        console.log(`Use ${searchMetadata.searchMask} for searching instead of ${searchMetadata.searchInputName}`);
        searchMetadata.searchInputName = searchMetadata.searchMask;
    }
    return searchMetadata;
}

const setSearchLibraries = (searchData)  => {
    switch (searchData.searchType) {
        case 'em2lm':
        case 'lmTarget':
            return {
                ...searchData,
                libraryAlignmentSpace: 'JRC2018_Unisex_20x_HR',
                searchableMIPSFolder: 'searchable_neurons',
                gradientsFolder: 'gradient',
                zgapMasksFolder: 'zgap',
                maskROIKey: null,
                libraries: [
                    'FlyLight_Split-GAL4_Drivers',
                    'FlyLight_Gen1_MCFO'
                ]
            };
        case 'lm2em':
        case 'emTarget':
            return {
                ...searchData,
                libraryAlignmentSpace: 'JRC2018_Unisex_20x_HR',
                searchableMIPSFolder: 'searchable_neurons',
                gradientsFolder: 'gradient',
                zgapMasksFolder: 'zgap',
                maskROIKey: maxHemibrainMask,
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

const startMonitor = async (searchId, monitorParams, stateMachineArn) => {
    const uniqueMonitorId = searchId || uuidv1();
    const timestamp = new Date().getTime();
    const monitorUniqueName = `ColorDepthSearch_${uniqueMonitorId}_${timestamp}`;
    await startStepFunction(
        monitorUniqueName,
        monitorParams,
        stateMachineArn
    );
    return monitorUniqueName;
}
