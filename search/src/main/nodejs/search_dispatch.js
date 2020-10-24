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
    ALIGNMENT_JOB_SUBMITTED,
    ALIGNMENT_JOB_COMPLETED,
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
const dispatchFunction = process.env.DISPATCH_FUNCTION_ARN;
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

const bc = new AWS.Batch();
const s3Retries = process.env.S3_RETRIES || 3;

const searchStarter = async (event) => {
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
            if (r.step < ALIGNMENT_JOB_COMPLETED) {
                console.log('Start alignment for', r);
                return await startAlignment(r);
            } else if (r.step >= ALIGNMENT_JOB_COMPLETED) {
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
        console.log('Returned results:', results);
        const statusResult = results.find(r => !!r.errorMessage);
        let httpStatusCode;
        let returnedResults;
        if (statusResult && !!statusResult.errorMessage) {
            httpStatusCode = statusResult.statusCode || 500;
            returnedResults = {
                errorMessage: statusResult.errorMessage,
                submissionResults: results
            };
        } else {
            httpStatusCode = 200;
            returnedResults = results;
        }

        return {
            statusCode: httpStatusCode,
            isBase64Encoded: false,
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(returnedResults)
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
    const limitsMessage = await checkLimits(searchParams, concurrentColorDepthSearchLimits, perDayColorDepthSearchLimits, s => s.step === SEARCH_IN_PROGRESS);
    if (limitsMessage) {
        console.log(`No color depth search started because ${limitsMessage}`, searchParams);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            errorMessage: `Color depth search was not started because ${limitsMessage}`
        });
        return {
            statusCode: 403,
            errorMessage: `Color depth search was not started because ${limitsMessage}`
        };
    }
    console.log('Start ColorDepthSearch', searchParams);
    const searchInputName = searchParams.searchMask
        ? searchParams.searchMask
        : searchParams.searchInputName

    searchParams.displayableMask = await createDisplayableMask(searchBucket, searchParams.searchInputFolder, searchInputName);
    if (searchParams.displayableMask) {
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            displayableMask: searchParams.displayableMask,
        });
    }
    return await dispatchColorDepthSearch(searchInputName, searchParams);
}

const createDisplayableMask = async (bucket, prefix, key) => {
    if (/\.(tiff?|gif|jpe?g|bmp)$/.test(key)) {
        const fullKey = `${prefix}/${key}`;
        try {
            console.log(`Convert ${bucket}:${key} to PNG`);
            const imageContent = await getS3ContentWithRetry(bucket, fullKey, s3Retries);
            const pngMime = "image/png";
            const pngExt = ".png";
            const image = await Jimp.read(imageContent);
            const imageBuffer = await image.getBufferAsync(pngMime);
            const pngImageName = getSearchKey(fullKey, pngExt);
            console.log(`Put ${bucket}:${pngImageName}`, imageBuffer);
            await putS3Content(bucket, pngImageName, pngMime, imageBuffer);
            console.info(`${fullKey} converted to png successfully`);
            return getSearchMaskId(pngImageName, pngExt);
        } catch (convertError) {
            console.error(`Error converting ${bucket}:${prefix}/${key} to PNG`, convertError);
            return null;
        }
    } else {
        return key;
    }
}

const dispatchColorDepthSearch = async (searchInputName, searchParams) => {
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

const startAlignment = async (searchParams) => {
    const limitsMessage = await checkLimits(searchParams, concurrentAlignmentLimits, perDayAlignmentLimits, s => s.step === ALIGNMENT_JOB_SUBMITTED);
    if (limitsMessage) {
        console.log(`No job invoked because ${limitsMessage}`, searchParams);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            errorMessage: `Alignment was not started because ${limitsMessage}`
        });
        return {
            statusCode: 403,
            errorMessage: `Alignment was not started because ${limitsMessage}`
        };
    }
    if (searchParams.simulateMIPGeneration) {
        return await generateMIPs(searchParams);
    } else {
        return await submitAlignmentJob(searchParams);
    }
}

const checkLimits = async (searchParams, concurrentSearches, perDayLimits, searchesFilter) => {
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
    const currentSearches =  searches.filter(searchesFilter);
    if (concurrentSearches >= 0 && currentSearches.length >=  concurrentSearches) {
        return `it is already running ${currentSearches.length} searches - the maximum allowed concurrent searches`;
    }
    return null;
}

const submitAlignmentJob = async (searchParams) => {
    const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;
    const searchInputMetadata = await getS3ContentMetadata(searchBucket, fullSearchInputImage);
    console.log('Search input metadata', searchInputMetadata);
    const searchInputSize = searchInputMetadata.ContentLength;
    const searchInputContentType = searchInputMetadata.ContentType;
    let estimatedMemory;
    if (searchInputContentType === 'application/zip') {
        estimatedMemory = searchInputSize / (1024. * 1024.) * 8 * 3.5;
    } else {
        estimatedMemory = searchInputSize / (1024. * 1024.) * 3.5;
    }
    const cpus = 16;
    const mem = Math.max(16 * 1024, Math.ceil(estimatedMemory));
    console.log(`Estimated memory for ${fullSearchInputImage}: ${estimatedMemory}, allocated memory: ${mem}`);
    const jobResources = {
        'vcpus': cpus,
        'memory': mem,
        'environment': [{
            name: 'ALIGNMENT_MEMORY',
            value: mem + 'M'
        }]
    };
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
        return {
            statusCode: 404,
            errorMessage: `Error submitting alignment job: ${submitError.message}`
        };
    }
}

module.exports = {
    searchStarter
}
