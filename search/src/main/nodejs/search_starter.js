'use strict';

const AWS = require('aws-sdk');
const {invokeAsync} = require('./utils');
const {getSearchMetadata, updateSearchMetadata, lookupSearchMetadata} = require('./awsappsyncutils');

const dispatchFunction = process.env.SEARCH_DISPATCH_FUNCTION;
const jobDefinition = process.env.JOB_DEFINITION;
const jobQueue = process.env.JOB_QUEUE;
const maxSearchesPerDay = process.env.MAX_SEARCHES_PER_DAY || 1

const bc = new AWS.Batch();

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
    const searchPromises = await newRecords.map(async r => {
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
            .map(async searchId => await getSearchMetadata(searchId))
            .filter(r => !!r);
        return await Promise.all(newRecordsPromises);
    } else if (e.searchIds) {
        const newSearchesPromises = await e.searchIds
            .map(async searchId => await getSearchMetadata(searchId))
            .filter(r => !!r);
        return await Promise.all(newSearchesPromises);
    } else if (e.searches) {
        return e.searches;
    } else {
        return [];
    }
}

const startColorDepthSearch = async (searchParams) => {
    if (!checkLimits(searchParams, maxSearchesPerDay)) {
        console.log("No color depth search started because the quota was exceeded", searchParams);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            errorMessage: `Color depth search was not started because the quota of ${maxSearchesPerDay} was exceeded`
        });
        return {};
    }
    console.log('Start ColorDepthSearch', searchParams);
    const cdsInvocationResult = await invokeAsync(dispatchFunction, searchParams);
    console.log('Started ColorDepthSearch', cdsInvocationResult);
    return cdsInvocationResult;
}

const startAlignment = async (searchParams) => {
    if (!checkLimits(searchParams, maxSearchesPerDay)) {
        console.log("No job invoked because the quota was exceeded", searchParams);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            errorMessage: `Alignment was not started because the quota of ${maxSearchesPerDay} was exceeded`
        });
        return {};
    }
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

const checkLimits = async (searchParams, limits) => {
    if (limits < 0) {
        return true;
    }
    const searches = await lookupSearchMetadata({
        identityId: searchParams.identityId,
        owner: searchParams.owner,
        maxStep: 4,
        withNoErrorsOnly: true,
        lastUpdated: new Date()
    });
    if (searches.length >= limits) {
        console.log(`The number of existing searches: ${searches.length} is greater than ${limits}`, searchParams);
        return false;
    } else {
        return true;
    }
}
