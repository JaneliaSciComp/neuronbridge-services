'use strict';

const AWS = require('aws-sdk');
const Jimp = require('jimp');
const {getSearchKey} = require('./searchutils');
const {getS3Content, invokeAsync, putS3Content} = require('./utils');
const {getSearchMetadata, updateSearchMetadata, lookupSearchMetadata} = require('./awsappsyncutils');

const dispatchFunction = process.env.SEARCH_DISPATCH_FUNCTION;
const jobDefinition = process.env.JOB_DEFINITION;
const jobQueue = process.env.JOB_QUEUE;
const perDaySearchLimits = process.env.MAX_SEARCHES_PER_DAY || 1
const concurrentSearchLimits = process.env.CONCURRENT_SEARCHES || 1;

const bc = new AWS.Batch();
const searchBucket = process.env.SEARCH_BUCKET;

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
        if (searchParams.upload.endsWith('.tif') ||
            searchParams.upload.endsWith('.tiff')) {
            const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;
            console.log(`Convert ${searchBucket}:${fullSearchInputImage} to PNG`);
            const imageContent = await getS3Content(searchBucket, fullSearchInputImage);
            const pngMime = 'image/png';
            await Jimp.read(imageContent)
                .then(image => image.getBuffer(pngMime))
                .then((imageBuffer) => {
                    const pngImageName = getSearchKey(fullSearchInputImage, '.png');
                    console.log(`Put ${searchBucket}:${pngImageName}`, imageBuffer);
                    return putS3Content(searchBucket, pngImageName, pngMime, imageBuffer);
                })
                .catch(err => {
                    throw err;
                })
                .finally(() => {
                    console.info(`${fullSearchInputImage} converted to png successfully`);
                })
            console.log(image);
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
        const jobName = `align-${searchParams.owner}-${searchParams.id}`;
        const jobParameters = {
            nchannels: searchParams.channel + '',
            xy_resolution: searchParams.voxelX + '',
            z_resolution: searchParams.voxelZ + '',
            search_id: searchParams.id,
            input_filename: searchParams.searchInput,
            output_folder: searchParams.searchInputFolder
        };
        const params = {
            jobDefinition: jobDefinition,
            jobQueue: jobQueue,
            jobName: jobName,
            containerOverrides: jobResources,
            parameters: jobParameters
        };
        // submit batch job
        console.log('Job parameters', params);
        const job = await bc.submitJob(params).promise();
        console.log('Submitted', job);
        console.log(`Job ${job.jobName} launched with id ${job.jobId}`, job);
        return job;
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
