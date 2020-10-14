'use strict';

const AWS = require('aws-sdk');
const Jimp = require('jimp');
const {getSearchMetadataKey, getSearchKey, getSearchMaskId} = require('./searchutils');
const {
  getS3ContentWithRetry,
  putS3Content,
  getObject,
  putObject,
  invokeAsync,
  verifyKey,
  startStepFunction,
  DEBUG,
} = require("./utils");
const {
  getSearchMetadata,
  updateSearchMetadata,
  lookupSearchMetadata,
  SEARCH_IN_PROGRESS, 
  ALIGNMENT_JOB_SUBMITTED,
} = require("./awsappsyncutils");
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
const dispatchFunction = process.env.DISPATCH_FUNCTION_ARN;
const searchFunction = process.env.SEARCH_FUNCTION;
const reduceFunction = process.env.REDUCE_FUNCTION;
const searchTimeoutSecs = process.env.SEARCH_TIMEOUT_SECS;
const jobDefinition = process.env.JOB_DEFINITION;
const jobQueue = process.env.JOB_QUEUE;
const maxParallelism = process.env.MAX_PARALLELISM || DEFAULTS.maxParallelism;
const perDaySearchLimits = process.env.MAX_SEARCHES_PER_DAY || DEFAULTS.perDaySearchLimits;
const concurrentSearchLimits = process.env.MAX_ALLOWED_CONCURRENT_SEARCHES || DEFAULTS.concurrentSearchLimits;
const alignMonitorStateMachineArn = process.env.ALIGN_JOB_STATE_MACHINE_ARN;

const defaultBatchSize = () => {
  if (process.env.BATCH_SIZE) {
    const configuredBatchSize = process.env.BATCH_SIZE * 1;
    return configuredBatchSize > 0 ? configuredBatchSize : DEFAULTS.batchSize;
  } else {
    return DEFAULTS.batchSize;
  }
};

const bc = new AWS.Batch();
const s3Retries = process.env.S3_RETRIES || 3;

const getCount = async (libraryBucket, libraryKey) => {
  if (DEBUG) console.log("Get count from:", libraryKey);
  const countMetadata = await getObject(
    libraryBucket,
    `${libraryKey}/counts_denormalized.json`,
    { objectCount: 0 }
  );
  return countMetadata.objectCount;
};

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

exports.searchStarter = async (event) => {
    console.log(event);
    let sourceIsHttpApiGateway;
    let eventBody;
    if (event.body) {
        eventBody = JSON.parse(event.body);
        console.log("Parsed body", eventBody)
        sourceIsHttpApiGateway = true;
    } else {
        eventBody = event;
        sourceIsHttpApiGateway = false;
    }
    const newRecords = await getNewRecords(eventBody);
    const searchPromises = await newRecords
        .filter(r => !!r)
        .map(async r => {
            if (r.step === 0) {
                console.log('Start alignment for', r);
                return await startAlignment(r);
            } else if (r.step === 2) {
                console.log('Start color depth search for', r);
                return await startColorDepthSearch(r);
            } else {
                // do nothing
                console.log('No processing for', r);
                return r;
            }
        });
    const results = await Promise.all(searchPromises);
    if (sourceIsHttpApiGateway) {
        return {
            isBase64Encoded: false,
            statusCode: 200,
            body: JSON.stringify(results)
        }
    } else {
        return results;
    }
}

const getNewRecords = async (e) => {
    if (e.Records) {
        const newRecordsPromises = await e.Records
            .filter(r => r.eventName === 'INSERT')
            .map(r => r.dynamodb)
            .map(r => r.Keys.id.S)
            .map(async searchId => await getSearchMetadata(searchId));
        return await Promise.all(newRecordsPromises);
    } else if (e.submittedSearches) {
        // this branch retrieves the searches from the DB
        // but if some fields are not yet set because of DynamoDB's eventual consistency
        // it sets those fields from the submittedSearch instead
        const newSearchesPromises = await e.submittedSearches
            .map(async submittedSearch => {
                let searchMetadata = await getSearchMetadata(submittedSearch.id || submittedSearch.searchId);
                Object.entries(submittedSearch)
                    .forEach(([key, value]) => {
                        if (value !== null && (searchMetadata[key] === null || searchMetadata[key] === undefined)) {
                            console.log(`Field ${key} not set`, searchMetadata, 'expected to be', value);
                            searchMetadata[key] = value;
                        }
                    });
                return searchMetadata;
            });
        return await Promise.all(newSearchesPromises);
    } else if (e.searches) {
        return e.searches;
    } else {
        return [];
    }
}

const startColorDepthSearch = async (searchParams) => {
    const limitsMessage = await checkLimits(searchParams, concurrentSearchLimits, perDaySearchLimits);
    if (limitsMessage) {
        console.log(`No color depth search started because ${limitsMessage}`, searchParams);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            errorMessage: `Color depth search was not started because ${limitsMessage}`
        });
        return {};
    }
    console.log("Start ColorDepthSearch", searchParams);
    const searchInputName = searchParams.searchMask ? searchParams.searchMask : searchParams.searchInputName;
    if (searchInputName.endsWith(".tif") || searchInputName.endsWith(".tiff")) {
        const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchInputName}`;
        try {
            console.log(`Convert ${searchBucket}:${fullSearchInputImage} to PNG`);
            const imageContent = await getS3ContentWithRetry(searchBucket, fullSearchInputImage, s3Retries);
            const pngMime = "image/png";
            const pngExt = ".png";
            const image = await Jimp.read(imageContent);
            const imageBuffer = await image.getBufferAsync(pngMime);
            const pngImageName = getSearchKey(fullSearchInputImage, pngExt);
            console.log(`Put ${searchBucket}:${pngImageName}`, imageBuffer);
            await putS3Content(searchBucket, pngImageName, pngMime, imageBuffer);
            console.info(`${fullSearchInputImage} converted to png successfully`);
            searchParams.displayableMask = getSearchMaskId(pngImageName, pngExt);
            await updateSearchMetadata({
                id: searchParams.id || searchParams.searchId,
                displayableMask: searchParams.displayableMask,
            });
        } catch (convertError) {
            console.error(
                `Error converting ${searchBucket}:${fullSearchInputImage} to PNG`,
                convertError
            );
        }
    } else {
        // the upload mask is displayable so set it as such
        searchParams.displayableMask = searchInputName;
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            displayableMask: searchParams.displayableMask,
        });
    }

    const searchInputParams = await getSearchInputParams(searchParams);
    const searchId = searchInputParams.searchId;
    const searchInputFolder = searchParams.searchInputFolder;
    const batchSize = parseInt(searchParams.batchSize) || defaultBatchSize();
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
        maskThreshold: parseInt(searchParams.maskThreshold) || DEFAULTS.maskThreshold,
        maxResultsPerMask: searchParams.maxResultsPerMask || DEFAULTS.maxResultsPerMask,
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
    console.log("Starting ColorDepthSearch with params", JSON.stringify(dispatchParams));
    const cdsInvocationResult = await invokeAsync(dispatchFunction, dispatchParams);
    console.log("Started ColorDepthSearch", cdsInvocationResult);
    if (cdsInvocationResult.FunctionError) {
        const errMsg = "Error launching burst compute job"
        console.log(`${errMsg}: ${cdsInvocationResult.FunctionError}`)
        console.log(JSON.parse(cdsInvocationResult.Payload))
        throw new Error(errMsg)
    }
    const response = JSON.parse(cdsInvocationResult.Payload)
    const jobId = response.jobId;
    const numBatches = response.numBatches;
    const branchingFactor = response.branchingFactor;
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
    const searchMetadataKey = getSearchMetadataKey(
        `${searchInputParamsWithLibraries.searchInputFolder}/${searchInputParamsWithLibraries.searchInputName}`)
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

const startAlignment = async (searchParams) => {
    const limitsMessage = await checkLimits(searchParams, concurrentSearchLimits, perDaySearchLimits);
    if (limitsMessage) {
        console.log(`No job invoked because ${limitsMessage}`, searchParams);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            errorMessage: `Alignment was not started because ${limitsMessage}`
        });
        return {};
    }
    if (searchParams.simulateMIPGeneration) {
        return await generateMIPs(searchParams);
    } else {
        return await submitAlignmentJob(searchParams);
    }
}

const checkLimits = async (searchParams, concurrentSearches, perDayLimits) => {
    if (concurrentSearches < 0 && perDayLimits < 0) {
        // no limits
        return null;
    }
    const searches = await lookupSearchMetadata({
        currentSearchId: searchParams.id,
        identityId: searchParams.identityId,
        owner: searchParams.owner,
        withNoErrorsOnly: true,
        lastUpdated: new Date()
    });
    if (perDayLimits >= 0 && searches.length >= perDayLimits) {
        return `it already reached the daily limits`;
    }
    const currentSearches =  searches.filter(s => s.step < 4);
    if (concurrentSearches >= 0 && currentSearches.length >=  concurrentSearches) {
        return `it is already running ${currentSearches.length} searches - the maximum allowed concurrent searches`;
    }
    return null;
}

const submitAlignmentJob = async (searchParams) => {
    const jobResources = {
        'vcpus': 16,
        'memory': 8192,
        'environment': [{
            name: 'ALIGNMENT_MEMORY',
            value: '8G'
        }]
    };
    const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;
    const jobName = `align-${searchParams.id}`;
    let jobParameters = {
        search_id: searchParams.id,
        input_filename: fullSearchInputImage,
        output_folder: searchParams.searchInputFolder
    };
    if (searchParams.userDefinedImageParams) {
        const xyRes = searchParams.voxelX ? searchParams.voxelX + '' : '1';
        const zRes = searchParams.voxelZ ? searchParams.voxelZ + '' : '1'
        const refChannel = searchParams.referenceChannel;
        jobParameters.force_voxel_size = 'true';
        jobParameters.xy_resolution = xyRes;
        jobParameters.z_resolution = zRes;
        jobParameters.reference_channel = refChannel;
    }
    const params = {
        jobDefinition: jobDefinition,
        jobQueue: jobQueue,
        jobName: jobName,
        containerOverrides: jobResources,
        parameters: jobParameters
    };
    console.log('Job parameters', params);
    try {
        // submit batch job
        const job = await bc.submitJob(params).promise();
        const now = new Date();
        console.log('Submitted', job);
        console.log(`Job ${job.jobName} launched with id ${job.jobId}`, job);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            step: ALIGNMENT_JOB_SUBMITTED,
            alignStarted: now.toISOString()
        });
        if (alignMonitorStateMachineArn != null) {
            // start the state machine
            const timestamp = now.getTime();
            await startStepFunction(
                `Align_${job.jobId}_${timestamp}`,
                {
                    searchId: searchParams.id || null,
                    jobId: job.jobId,
                    startTime: timestamp
                },
                alignMonitorStateMachineArn
            );
        }
        return job;
    } catch (submitError) {
        console.error('Error submitting job with parameters', params, submitError);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            step: ALIGNMENT_JOB_SUBMITTED,
            errorMessage: `Error submitting alignment job for ${searchParams.id}:${fullSearchInputImage} - ${submitError.message}`
        });
        throw submitError;
    }
}
