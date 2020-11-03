import AWS from 'aws-sdk';
import Jimp from 'jimp';
import {getSearchKey, getSearchMaskId} from './searchutils';
import {
    getS3ContentWithRetry,
    getS3ContentMetadata,
    putS3Content,
    startStepFunction,
} from './utils';
import {
    ALIGNMENT_JOB_SUBMITTED,
    ALIGNMENT_JOB_COMPLETED,
    SEARCH_IN_PROGRESS,
    getSearchMetadata,
    lookupSearchMetadata,
    updateSearchMetadata
} from './awsappsyncutils';
import {generateMIPs} from './mockMIPGeneration';
import {cdsStarter} from './cds_starter';

const jobDefinition = process.env.JOB_DEFINITION;
const jobQueue = process.env.JOB_QUEUE;
const perDayColorDepthSearchLimits = process.env.MAX_SEARCHES_PER_DAY || 1;
const concurrentColorDepthSearchLimits = process.env.MAX_ALLOWED_CONCURRENT_SEARCHES || 1;
const perDayAlignmentLimits = process.env.MAX_ALIGNMENTS_PER_DAY || 1;
const concurrentAlignmentLimits = process.env.MAX_ALLOWED_CONCURRENT_ALIGNMENTS || 1;
const alignMonitorStateMachineArn = process.env.ALIGN_JOB_STATE_MACHINE_ARN;
const searchBucket = process.env.SEARCH_BUCKET;

const bc = new AWS.Batch();

export const appStarter = async (event) => {
    console.log(event);
    let sourceIsHttpApiGateway;
    let eventBody;
    if (event.body) {
        eventBody = JSON.parse(event.body);
        console.log("Parsed body", eventBody);
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
        };
    } else {
        return results;
    }
};

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
};

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
    } else {
        console.log('Start ColorDepthSearch', searchParams);
        const searchInputName = searchParams.searchMask
            ? searchParams.searchMask
            : searchParams.searchInputName;

        searchParams.displayableMask = await createDisplayableMask(searchBucket, searchParams.searchInputFolder, searchInputName);
        if (searchParams.displayableMask) {
            await updateSearchMetadata({
                id: searchParams.id || searchParams.searchId,
                displayableMask: searchParams.displayableMask,
            });
        }
        searchParams.searchBucket = searchBucket;
        const cdsInvocationResult = await cdsStarter(searchParams);
        console.log('Started ColorDepthSearch', cdsInvocationResult);
        return cdsInvocationResult;
    }
};

const createDisplayableMask = async (bucket, prefix, key) => {
    if (/\.(tiff?|gif|jpe?g|bmp)$/.test(key)) {
        const fullKey = `${prefix}/${key}`;
        try {
            console.log(`Convert ${bucket}:${key} to PNG`);
            const imageContent = await getS3ContentWithRetry(bucket, fullKey);
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
    } else {
        if (searchParams.simulateMIPGeneration) {
            return await generateMIPs(searchParams);
        } else {
            return await submitAlignmentJob(searchParams);
        }
    }
};

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
};

const submitAlignmentJob = async (searchParams) => {
    const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;
    const searchInputMetadata = await getS3ContentMetadata(searchBucket, fullSearchInputImage);
    console.log('Search input metadata', searchInputMetadata);
    const searchInputSize = searchInputMetadata.ContentLength;
    const searchInputContentType = searchInputMetadata.ContentType;
    const comparisonAlgorithm = searchInputMetadata.algorithm === 'avg' ? 'Median' : 'Max';
    let estimatedMemory;
    if (searchInputContentType === 'application/zip') {
        estimatedMemory = searchInputSize / (1024.0 * 1024.0) * 4 * 8;
        console.log(`Estimate memory for zip files to ${estimatedMemory}`);
    } else if (fullSearchInputImage.toLowerCase().endsWith('.h5j')) {
        // for h5j we consider a compression factor of "only" 32
        // so in some cases this may result in OOM because there are situations when
        // the compression factor may be ~200x
        estimatedMemory = searchInputSize / (1024.0 * 1024.0) * 4 * 32;
        console.log(`Estimate memory for h5j files to ${estimatedMemory}`);
    } else {
        estimatedMemory = searchInputSize / (1024.0 * 1024.0) * 4;
    }
    const mem = Math.max(16 * 1024, Math.ceil(estimatedMemory));
    let cpus;
    if (mem >= 32 * 1024) {
        cpus = 32;
    } else {
        cpus = 16;
    }
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
        output_folder: searchParams.searchInputFolder,
        comparison_alg: comparisonAlgorithm,
        nslots: cpus + ''
    };
    if (searchParams.userDefinedImageParams) {
        const xyRes = searchParams.voxelX ? searchParams.voxelX + '' : '1';
        const zRes = searchParams.voxelZ ? searchParams.voxelZ + '' : '1';
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
};
