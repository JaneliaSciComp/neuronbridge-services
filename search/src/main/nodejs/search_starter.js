'use strict';

const AWS = require('aws-sdk');
const Jimp = require('jimp');
const {getSearchKey, getSearchMaskId} = require('./searchutils');
const {getS3ContentWithRetry, invokeAsync, putS3Content, startStepFunction} = require('./utils');
const {getSearchMetadata, updateSearchMetadata, lookupSearchMetadata, ALIGNMENT_JOB_SUBMITTED} = require('./awsappsyncutils');

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
            if (r.step === 0) {
                return await startAlignment(r);
            } else if (r.step === 2) {
                return await startColorDepthSearch(r);
            } else {
                // do nothing
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
    } else if (e.searchIds) {
        const newSearchesPromises = await e.searchIds
            .map(async searchId => await getSearchMetadata(searchId))
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
        if (searchParams.searchInputName.endsWith('.tif') ||
            searchParams.searchInputName.endsWith('.tiff')) {
            const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;
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
            searchParams.displayableMask = searchParams.searchInputName;
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
        const jobResources = {
            'vcpus': 16,
            'memory': 8192
        };
        const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;
        const jobName = `align-${searchParams.id}`;
        const jobParameters = {
            nchannels: searchParams.channel + '',
            xy_resolution: searchParams.voxelX + '',
            z_resolution: searchParams.voxelZ + '',
            search_id: searchParams.id,
            input_filename: fullSearchInputImage,
            output_folder: searchParams.searchInputFolder
        };
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
            console.log('Submitted', job);
            console.log(`Job ${job.jobName} launched with id ${job.jobId}`, job);
            await updateSearchMetadata({
                id: searchParams.id || searchParams.searchId,
                step: ALIGNMENT_JOB_SUBMITTED,
            });
            if (alignMonitorStateMachineArn != null) {
                // start the state machine
                const now = new Date().getTime();
                await startStepFunction(
                    `Align_${job.jobId}_${now}`,
                    {
                        searchId: searchParams.id || null,
                        jobId: job.jobId,
                        startTime: now
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
