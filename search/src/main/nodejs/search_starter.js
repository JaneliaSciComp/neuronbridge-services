'use strict';

const AWS = require('aws-sdk');
const Jimp = require('jimp');
const {getSearchKey, getSearchMaskId} = require('./searchutils');
const {getS3ContentWithRetry, invokeAsync, putS3Content, startStepFunction} = require('./utils');
const {getSearchMetadata, updateSearchMetadata, lookupSearchMetadata, ALIGNMENT_JOB_SUBMITTED, ALIGNMENT_JOB_COMPLETED} = require('./awsappsyncutils');
const {generateMIPs} = require('./mockMIPGeneration');

const dispatchFunction = process.env.SEARCH_DISPATCH_FUNCTION;
const jobDefinition = process.env.JOB_DEFINITION;
const jobQueue = process.env.JOB_QUEUE;
const perDaySearchLimits = process.env.MAX_SEARCHES_PER_DAY || 1
const concurrentSearchLimits = process.env.MAX_ALLOWED_CONCURRENT_SEARCHES || 1;
const alignMonitorStateMachineArn = process.env.ALIGN_JOB_STATE_MACHINE_ARN;

const bc = new AWS.Batch();
const searchBucket = process.env.SEARCH_BUCKET;
const s3Retries = process.env.S3_RETRIES || 3;

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
    } else {
        console.log('Start ColorDepthSearch', searchParams);
        const searchInputName = searchParams.searchMask
            ? searchParams.searchMask
            : searchParams.searchInputName
        if (searchInputName.endsWith('.tif') ||
            searchInputName.endsWith('.tiff')) {
            const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchInputName}`;
            try {
                console.log(`Convert ${searchBucket}:${fullSearchInputImage} to PNG`);
                const imageContent = await getS3ContentWithRetry(searchBucket, fullSearchInputImage, s3Retries);
                const pngMime = 'image/png';
                const pngExt = '.png';
                const image = await Jimp.read(imageContent);
                const imageBuffer = await image.getBufferAsync(pngMime);
                const pngImageName = getSearchKey(fullSearchInputImage, pngExt);
                console.log(`Put ${searchBucket}:${pngImageName}`, imageBuffer);
                await putS3Content(searchBucket, pngImageName, pngMime, imageBuffer);
                console.info(`${fullSearchInputImage} converted to png successfully`);
                searchParams.displayableMask = getSearchMaskId(pngImageName, pngExt);
                await updateSearchMetadata({
                    id: searchParams.id || searchParams.searchId,
                    displayableMask: searchParams.displayableMask
                });
            } catch (convertError) {
                console.error(`Error converting ${searchBucket}:${fullSearchInputImage} to PNG`, convertError);
            }
        } else {
            // the upload mask is displayable so set it as such
            searchParams.displayableMask = searchInputName;
            await updateSearchMetadata({
                id: searchParams.id || searchParams.searchId,
                displayableMask: searchParams.displayableMask
            });
        }
        const cdsInvocationResult = await invokeAsync(dispatchFunction, searchParams);
        console.log('Started ColorDepthSearch', cdsInvocationResult);
        return cdsInvocationResult;
    }
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
    } else {
        if (searchParams.simulateMIPGeneration) {
            return await generateMIPs(searchParams);
        } else {
            return await submitAlignmentJob(searchParams);
        }
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
    const cpus = 16;
    const mem = 16 * 1024; // 16M
    const jobResources = {
        'vcpus': cpus,
        'memory': mem,
        'environment': [{
            name: 'ALIGNMENT_MEMORY',
            value: mem + 'M'
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
