import {getSearchMetadataKey} from './searchutils';
import {
    DEBUG,
    invokeAsync,
    putObject,
} from './utils';
import {
    SEARCH_IN_PROGRESS,
    updateSearchMetadata
} from './awsappsyncutils';
import {
    getSearchInputParams,
    checkSearchMask,
    getSearchedLibraries
} from './cds_input';

const DEFAULTS = {
  maxParallelism: 3000,
  perDaySearchLimits: 1,
  concurrentSearchLimits: 1,
  batchSize: 40
};

const dataBucket = process.env.DATA_BUCKET;
const defaultSearchBucket = process.env.SEARCH_BUCKET;
const parallelDispatchFunction = process.env.PARALLEL_DISPATCH_FUNCTION_ARN;
const searchFunction = process.env.SEARCH_FUNCTION;
const reduceFunction = process.env.REDUCE_FUNCTION;
const searchTimeoutSecs = process.env.SEARCH_TIMEOUT_SECS;

const maxParallelism = process.env.MAX_PARALLELISM || DEFAULTS.maxParallelism;

const defaultBatchSize = () => {
  if (process.env.BATCH_SIZE) {
    const configuredBatchSize = process.env.BATCH_SIZE * 1;
    return configuredBatchSize > 0 ? configuredBatchSize : DEFAULTS.batchSize;
  } else {
    return DEFAULTS.batchSize;
  }
};

export const cdsStarter = async (event) => {
    console.log('Input event:', JSON.stringify(event));

    const searchInputParams = await getSearchInputParams(event);
    if (DEBUG) console.log('Input params:', searchInputParams);

    const searchId = searchInputParams.searchId;
    const searchBucket = searchInputParams.searchBucket || defaultSearchBucket;

    const searchInputName = searchInputParams.searchMask
        ? searchInputParams.searchMask
        : searchInputParams.searchInputName;

    const searchInputFolder = searchInputParams.searchInputFolder;
    const batchSize = parseInt(searchInputParams.batchSize) || defaultBatchSize();
    const maskKey = `${searchInputFolder}/${searchInputName}`;
    await checkSearchMask(searchId, searchBucket, maskKey);
    const searchedData = await getSearchedLibraries(searchInputParams, dataBucket);
    console.log("Search input params with libraries", searchedData);
    if (searchedData.totalSearches === 0) {
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
        searchId,
        dataThreshold: parseInt(searchInputParams.dataThreshold),
        pixColorFluctuation: parseFloat(searchInputParams.pixColorFluctuation),
        xyShift: parseInt(searchInputParams.xyShift),
        mirrorMask: searchInputParams.mirrorMask,
        minMatchingPixRatio: searchInputParams.minMatchingPixRatio,
        maskThresholds: [parseInt(searchInputParams.maskThreshold)],
        maxResultsPerMask: searchInputParams.maxResultsPerMask,
        searchBucket,
        maskKeys: [maskKey],
        inputAnatomicalRegion: searchedData.anatomicalRegion,
        targetType: searchedData.targetType,
        libraries: searchedData.searchedLibraries,
    };
    // Schedule the burst compute job
    const dispatchParams = {
        workerFunctionName: searchFunction,
        combinerFunctionName: reduceFunction,
        batchSize: batchSize,
        maxParallelism: maxParallelism,
        searchTimeoutSecs,
        jobParameters: jobParams,
        startIndex: 0,
        endIndex: searchedData.totalSearches,
    };
    console.log('Starting ColorDepthSearch with:', dispatchParams);
    const cdsInvocationResult = await invokeAsync(parallelDispatchFunction, dispatchParams);
    if (DEBUG) console.log(`Invoke ${parallelDispatchFunction} result:`, cdsInvocationResult);
    if (cdsInvocationResult.FunctionError) {
        const errMsg = "Error launching burst compute job";
        console.log(`${errMsg}: ${cdsInvocationResult.FunctionError}`, cdsInvocationResult);
        throw new Error(errMsg);
    }
    console.log("Started ColorDepthSearch", cdsInvocationResult.Payload);
    const jobId = cdsInvocationResult.Payload.jobId;
    const numBatches = cdsInvocationResult.Payload.numBatches;
    const branchingFactor = cdsInvocationResult.Payload.branchingFactor;
    // Persist the search metadata on S3
    const now = new Date();
    const searchMetadata = {
        startTime: now.toISOString(),
        searchType: searchedData.searchType,
        parameters: searchedData,
        nsearches: searchedData.totalSearches,
        branchingFactor: branchingFactor,
        partitions: numBatches,
        jobId
    };
    const searchMetadataKey = getSearchMetadataKey(`${searchInputFolder}/${searchInputName}`);
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
};
