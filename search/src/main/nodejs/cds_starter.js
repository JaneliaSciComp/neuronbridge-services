import {getSearchMetadataKey} from './searchutils';
import {
    DEBUG,
    getObjectWithRetry,
    invokeFunction,
    putObject,
    verifyKey
} from './utils';
import {
    SEARCH_IN_PROGRESS,
    getSearchMetadata,
    updateSearchMetadata
} from './awsappsyncutils';
import cdsConfig from '../../../cds_config.json'

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

const defaultLibraryBucket = process.env.LIBRARY_BUCKET;
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
    const libraryBucket = searchInputParams.libraryBucket || defaultLibraryBucket;

    const searchInputName = searchInputParams.searchMask
        ? searchInputParams.searchMask
        : searchInputParams.searchInputName;

    const searchInputFolder = searchInputParams.searchInputFolder;
    const batchSize = parseInt(searchInputParams.batchSize) || defaultBatchSize();
    const maskKey = `${searchInputFolder}/${searchInputName}`;
    await checkSearchMask(searchId, searchBucket, maskKey);
    const searchInputParamsWithLibraries = setSearchLibraries(searchInputParams);
    console.log("Search input params with libraries", searchInputParamsWithLibraries);
    const librariesPromises = await searchInputParamsWithLibraries.libraries
        .map(lname => {
            // for searching use MIPs from the searchableMIPs folder
            const libraryAlignmentSpace = searchInputParamsWithLibraries.libraryAlignmentSpace
                ? `${searchInputParamsWithLibraries.libraryAlignmentSpace}/`
                : '';
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
        searchId,
        dataThreshold: parseInt(searchInputParams.dataThreshold) || DEFAULTS.dataThreshold,
        pixColorFluctuation: parseFloat(searchInputParams.pixColorFluctuation) || DEFAULTS.pixColorFluctuation,
        xyShift: parseInt(searchInputParams.xyShift) || DEFAULTS.xyShift,
        mirrorMask: searchInputParams.mirrorMask || DEFAULTS.mirrorMask,
        minMatchingPixRatio: searchInputParams.minMatchingPixRatio || DEFAULTS.minMatchingPixRatio,
        maskThresholds: [parseInt(searchInputParams.maskThreshold) || DEFAULTS.maskThreshold],
        maxResultsPerMask: searchInputParams.maxResultsPerMask || DEFAULTS.maxResultsPerMask,
        searchBucket,
        maskKeys: [maskKey],
        libraryBucket,
        libraries: libraries.map(l => l.lkey),
        gradientsFolders: libraries.map(l => l.gradientsFolder),
        zgapMasksFolders: libraries.map(l => l.zgapMasksFolder)
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
        endIndex: totalSearches,
    };
    console.log('Starting ColorDepthSearch with:', dispatchParams);
    const cdsInvocationResult = await invokeFunction(parallelDispatchFunction, dispatchParams);
    if (cdsInvocationResult.FunctionError) {
        const errMsg = "Error launching burst compute job";
        console.log(`${errMsg}: ${cdsInvocationResult.FunctionError}`);
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
        searchType: searchInputParamsWithLibraries.searchType,
        parameters: searchInputParamsWithLibraries,
        libraries: libraries,
        nsearches: totalSearches,
        branchingFactor: branchingFactor,
        partitions: numBatches,
        jobId
    };
    const searchMetadataKey = getSearchMetadataKey(`${searchInputParamsWithLibraries.searchInputFolder}/${searchInputParamsWithLibraries.searchInputName}`);
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
};

/**
 * Create search libraries based on anatomicalRegion and searchType from cdsConfig.
 *
 * @param searchData
 * @returns {*&{libraries: (*|*[]), libraryAlignmentSpace: *, searchableMIPSFolder: *}}
 */
const setSearchLibraries = (searchData) => {
    const anatomicalRegion = searchData.anatomicalRegion || 'brain';
    console.log(`Getting search libraries for ${anatomicalRegion}:${searchData.searchType}`);
    const searchCfg = cdsConfig.filter(cfg => cfg.area === anatomicalRegion.toLowerCase());
    let searchLibraries;
    switch (searchData.searchType) {
        case "em2lm":
        case "lmTarget":
            searchLibraries = searchCfg.lmLibraries;
            break;
        case "lm2em":
        case "emTarget":
            searchLibraries = searchCfg.emLibraries;
            break;
        default:
            searchLibraries = searchData.libraries || [];
            break;
    }
    return {
        ...searchData,
        libraryAlignmentSpace: searchCfg.alignmentSpace,
        searchableMIPSFolder: searchCfg.searchFolder,
        libraries: searchLibraries,
    };

};

const getCount = async (libraryBucket, libraryKey) => {
    if (DEBUG) console.log("Get count from:", libraryKey);
    const countMetadata = await getObjectWithRetry(
        libraryBucket,
        `${libraryKey}/counts_denormalized.json`
    );
    return countMetadata.objectCount;
};
