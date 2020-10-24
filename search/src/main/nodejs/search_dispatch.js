'use strict';

const AWS = require('aws-sdk');
const Jimp = require('jimp');
const {
    getSearchKey,
    getSearchMaskId,
    getSearchMetadataKey
} = require('./searchutils');
const {
    DEBUG,
    getObject,
    getS3ContentWithRetry,
    getS3ContentMetadata,
    invokeAsync,
    putObject,
    putS3Content,
    startStepFunction,
    verifyKey
} = require('./utils');
const {
    SEARCH_IN_PROGRESS,
    getSearchMetadata,
    lookupSearchMetadata,
    updateSearchMetadata
} = require('./awsappsyncutils');
const {generateMIPs} = require('./mockMIPGeneration');

const DEFAULTS = {
  maskThreshold: 100,
  dataThreshold: 100,
  pixColorFluctuation: 2.0,
  xyShift: 2,
  mirrorMask: true,
  minMatchingPixRatio: 2,
  maxResultsPerMask: -1,
  maxParallelism: 3000,
  perDaySearchLimits: 1,
  concurrentSearchLimits: 1,
  batchSize: 40
};

const libraryBucket = process.env.LIBRARY_BUCKET;
const searchBucket = process.env.SEARCH_BUCKET;
const dispatchFunction = process.env.PARALLEL_DISPATCH_FUNCTION_ARN;
const searchFunction = process.env.SEARCH_FUNCTION;
const reduceFunction = process.env.REDUCE_FUNCTION;
const searchTimeoutSecs = process.env.SEARCH_TIMEOUT_SECS;
const jobDefinition = process.env.JOB_DEFINITION;
const jobQueue = process.env.JOB_QUEUE;

const maxParallelism = process.env.MAX_PARALLELISM || DEFAULTS.maxParallelism;
const perDayColorDepthSearchLimits = process.env.MAX_SEARCHES_PER_DAY || 1
const concurrentColorDepthSearchLimits = process.env.MAX_ALLOWED_CONCURRENT_SEARCHES || 1;
const perDayAlignmentLimits = process.env.MAX_ALIGNMENTS_PER_DAY || 1
const concurrentAlignmentLimits = process.env.MAX_ALLOWED_CONCURRENT_ALIGNMENTS || 1;

const alignMonitorStateMachineArn = process.env.ALIGN_JOB_STATE_MACHINE_ARN;

const defaultBatchSize = () => {
  if (process.env.BATCH_SIZE) {
    const configuredBatchSize = process.env.BATCH_SIZE * 1;
    return configuredBatchSize > 0 ? configuredBatchSize : DEFAULTS.batchSize;
  } else {
    return DEFAULTS.batchSize;
  }
};

const cdsStarter = async (searchInputName, searchParams) => {
    const searchInputParams = await getSearchInputParams(searchParams);
    const searchId = searchInputParams.searchId;
    const searchInputFolder = searchParams.searchInputFolder;
    const batchSize = parseInt(searchParams.batchSize) || defaultBatchSize();
    const maskKey = `${searchInputFolder}/${searchInputName}`;
    await checkSearchMask(searchId, searchBucket, maskKey);
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
            const library = {
                lname: lname,
                lkey: searchableMIPSFolder
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
    const libraries =  await Promise.all(librariesPromises);
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
    const jobParams = {
        dataThreshold: parseInt(searchParams.dataThreshold) || DEFAULTS.dataThreshold,
        pixColorFluctuation: parseFloat(searchParams.pixColorFluctuation) || DEFAULTS.pixColorFluctuation,
        xyShift: parseInt(searchParams.xyShift) || DEFAULTS.xyShift,
        mirrorMask: searchParams.mirrorMask || DEFAULTS.mirrorMask,
        minMatchingPixRatio: searchParams.minMatchingPixRatio || DEFAULTS.minMatchingPixRatio,
        maskThresholds: [parseInt(searchParams.maskThreshold) || DEFAULTS.maskThreshold],
        maxResultsPerMask: searchParams.maxResultsPerMask || DEFAULTS.maxResultsPerMask,
        maskPrefix: searchBucket,
        maskKeys: [maskKey],
        libraryBucket,
        searchBucket,
        libraries,
        searchId
    }
    // Schedule the burst compute job
    const dispatchParams = {
        workerFunctionName: searchFunction,
        combinerFunctionName: reduceFunction,
        batchSize: batchSize,
        maxParallelism: maxParallelism,
        searchTimeoutSecs,
        jobParameters: jobParams,
        startIndex: 0,
        endIndex: totalSearches,
    };
    console.log('Starting ColorDepthSearch with:', dispatchParams);
    const cdsInvocationResult = await invokeAsync(dispatchFunction, dispatchParams);
    console.log("Started ColorDepthSearch", cdsInvocationResult);
    const jobId = cdsInvocationResult.jobId;
    const numBatches = cdsInvocationResult.numBatches;
    const branchingFactor = cdsInvocationResult.branchingFactor;
    // Persist the search metadata on S3
    const now = new Date();
    const searchMetadata = {
        startTime: now.toISOString(),
        searchType: searchInputParamsWithLibraries.searchType,
        parameters: searchInputParamsWithLibraries,
        libraries: libraries,
        nsearches: totalSearches,
        branchingFactor: branchingFactor,
        partitions: numBatches,
        jobId
    };
    const searchMetadataKey = getSearchMetadataKey(`${searchInputParamsWithLibraries.searchInputFolder}/${searchInputParamsWithLibraries.searchInputName}`)
    await putObject(searchBucket, searchMetadataKey, searchMetadata);
    // Update search metadata if searchId is provided
    await updateSearchMetadata({
        id: searchId,
        step: SEARCH_IN_PROGRESS,
        maskThreshold: jobParams.maskThreshold,
        dataThreshold: jobParams.dataThreshold,
        pixColorFluctuation: jobParams.pixColorFluctuation,
        xyShift: jobParams.xyShift,
        mirrorMask: jobParams.mirrorMask,
        minMatchingPixRatio: jobParams.minMatchingPixRatio,
        maxResultsPerMask: jobParams.maxResultsPerMask,
        nBatches: numBatches,
        completedBatches: 0,
        cdsStarted: now.toISOString()
    });

    return cdsInvocationResult;
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
        console.log(
            `Use ${searchMetadata.searchMask} for searching instead of ${searchMetadata.searchInputName}`
        );
        searchMetadata.searchInputName = searchMetadata.searchMask;
    }
    return searchMetadata;
};

const checkSearchMask = async (searchId, bucket, maskKey) => {
    const checkMaskFlag = await verifyKey(bucket, maskKey);
    if (checkMaskFlag === false) {
        const errMsg = `Mask s3://${bucket}/${maskKey} not found`;
        // set the error
        await updateSearchMetadata({
            id: searchId,
            step: SEARCH_IN_PROGRESS,
            errorMessage: errMsg
        });
        throw new Error(errMsg);
    }
}

const setSearchLibraries = (searchData) => {
    switch (searchData.searchType) {
        case "em2lm":
        case "lmTarget":
            return {
                ...searchData,
                libraryAlignmentSpace: "JRC2018_Unisex_20x_HR",
                searchableMIPSFolder: "searchable_neurons",
                libraries: ["FlyLight_Split-GAL4_Drivers", "FlyLight_Gen1_MCFO"],
            };
        case "lm2em":
        case "emTarget":
            return {
                ...searchData,
                libraryAlignmentSpace: "JRC2018_Unisex_20x_HR",
                searchableMIPSFolder: "searchable_neurons",
                libraries: ["FlyEM_Hemibrain_v1.1"],
            };
        default:
            return {
                ...searchData,
                libraries: searchData.libraries || [],
            };
    }
};

const getCount = async (libraryBucket, libraryKey) => {
    if (DEBUG) console.log("Get count from:", libraryKey);
    const countMetadata = await getObject(
        libraryBucket,
        `${libraryKey}/counts_denormalized.json`,
        { objectCount: 0 }
    );
    return countMetadata.objectCount;
};

module.exports = {
    cdsStarter
}
