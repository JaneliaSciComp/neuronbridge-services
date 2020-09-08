'use strict';

const AWS = require('aws-sdk');
const moment = require('moment');

const {getIntermediateSearchResultsPrefix, getIntermediateSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const {getAllKeys, putText, DEBUG} = require('./utils');
const {updateSearchMetadata, ALIGNMENT_JOB_COMPLETED} = require('./awsappsyncutils');

const bc = new AWS.Batch({
    apiVersion: '2016-08-10'
});
const SEARCH_TIMEOUT_SECS = process.env.SEARCH_TIMEOUT_SECS;

exports.isSearchDone = async (event) =>  {
    console.log(event);
    try {
        if (event.jobId) {
            return await monitorAlignmentJob(event);
        } else {
            return await monitoCDSJob(event);
        }
    } catch (e) {
        console.log('Error while checking if search completed', event, e);
        return {
            ...event,
            completed: true,
            withErrors: true
        };
    }
}

const monitorAlignmentJob = async (alignJobParams) => {
    const searchId = alignJobParams.searchId;
    const jobId = alignJobParams.jobId;

    const jobDescriptions = await bc.describeJobs({
        jobs: [jobId]
    }).promise();
    console.log('Jobs', jobDescriptions);
    if (!jobDescriptions.jobs)  {
        console.log('Something must have gone completely wrong - no jobs found for', alignJobParams);
        await updateSearchMetadata({
            id: searchId,
            errorMessage: `No jobs found for ${jobId} created for search ${searchId}`
        });
        return {
            ...alignJobParams,
            completed: true,
            withErrors: true
        };
    }
    const job = jobDescriptions.jobs.find(j => j.jobId === jobId);
    if (job) {
        if (job.status === 'SUCCEEDED') {
            await updateSearchMetadata({
                id: searchId,
                step: ALIGNMENT_JOB_COMPLETED
            });
            return {
                ...alignJobParams,
                completed: true,
                withErrors: false
            };
        } else if (job.status === 'FAILED') {
            const searchMetadata = getSearchMetadata(searchId);
            if (!searchMetadata.errorMessage) {
                await updateSearchMetadata({
                    id: searchId,
                    errorMessage: 'Alignment job failed'
                });
            }
            return {
                ...alignJobParams,
                completed: true,
                withErrors: true
            };
        } else {
            // job is still running
            return {
                ...alignJobParams,
                completed: false,
                withErrors: false
            };
        }
    } else {
        // job not found
        console.log('No job not found for', alignJobParams);
        return {
            ...alignJobParams,
            completed: true,
            withErrors: true
        };
    }
}

const monitoCDSJob = async (cdsJobParams) => {

    // Parameters
    const bucket = cdsJobParams.bucket;
    const searchId = cdsJobParams.searchId;
    const searchInputFolder = cdsJobParams.searchInputFolder;
    const searchInputName = cdsJobParams.searchInputName;
    const numBatches = cdsJobParams.numBatches;
    const startTime = moment(cdsJobParams.startTime);

    if (!bucket) {
        throw new Error('Missing required key \'bucket\' in input to isSearchDone');
    }

    if (!searchInputName) {
        throw new Error('Missing required key \'searchInputName\' in input to isSearchDone');
    }

    const fullSearchInputName = `${searchInputFolder}/${searchInputName}`;
    const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(fullSearchInputName);

    // Fetch all keys
    const allKeys  = await getAllKeys({
        Bucket: bucket,
        Prefix: intermediateSearchResultsPrefix
    });
    const allBatchResultsKeys = new Set(allKeys);

    // Check if all partitions have completed
    let numComplete = 0;
    for(let batchIndex = 0; batchIndex < numBatches; batchIndex++) {
        const batchResultsKey = getIntermediateSearchResultsKey(fullSearchInputName, batchIndex);
        if (allBatchResultsKeys.has(batchResultsKey)) {
            numComplete++;
        }
    }

    console.log(`Completed: ${numComplete}/${numBatches}`);

    // Calculate total search time
    const now = new Date();
    const endTime = moment(now.toISOString());
    const elapsedSecs = endTime.diff(startTime, "s");
    const numRemaining = numBatches - numComplete;
    // write down the progress
    await updateSearchMetadata({
        id: searchId,
        completedBatches: numComplete
    });
    // return result for next state input
    if (numRemaining === 0) {
        console.log(`Search took ${elapsedSecs} seconds`);
        return {
            ...cdsJobParams,
            elapsedSecs: elapsedSecs,
            numRemaining: 0,
            completed: true,
            timedOut: false
        };
    } else if (elapsedSecs > SEARCH_TIMEOUT_SECS) {
        console.log(`Search timed out after ${elapsedSecs} seconds`);
        // update the error
        await updateSearchMetadata({
            id: searchId,
            errorMessage: `Search timed out after ${elapsedSecs} seconds`
        });
        return {
            ...cdsJobParams,
            elapsedSecs: elapsedSecs,
            numRemaining: numRemaining,
            completed: false,
            timedOut: true
        };
    } else {
        console.log(`Search still running after ${elapsedSecs} seconds. Completed ${numComplete} of ${numBatches} jobs.`);
        return {
            ...cdsJobParams,
            elapsedSecs: elapsedSecs,
            numRemaining: numRemaining,
            completed: false,
            timedOut: false
        };
    }

}