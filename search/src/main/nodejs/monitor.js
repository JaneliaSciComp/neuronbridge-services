'use strict';

const AWS = require('aws-sdk');
const moment = require('moment');

const {getIntermediateSearchResultsPrefix, getIntermediateSearchResultsKey, getSearchProgressKey} = require('./searchutils');
const {tagS3Content} = require('./utils');
const {getAllKeys, DEBUG} = require('./utils');
const {getSearchMetadata, updateSearchMetadata, ALIGNMENT_JOB_COMPLETED} = require('./awsappsyncutils');

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
        await updateSearchMetadata({
            id: event.searchId,
            errorMessage: `Error while checking search status ${event.searchId} - ${e.message}`
        });
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
        if (DEBUG) {
            console.log(`Job ${job.jobId} ${job.status}`);
        }
        const timestamp = new Date();
        if (job.status === 'SUCCEEDED') {
            await updateSearchMetadata({
                id: searchId,
                step: ALIGNMENT_JOB_COMPLETED,
                alignFinished: timestamp.toISOString()
            });
            console.log('Get metadata for search ID ', searchId);
            const searchMetadata = await getSearchMetadata(searchId);
            const tags = {TagSet: [{Key: "LIFECYCLE", 
                                    Value: "DELETE"}]}
            await tagS3Content(alignJobParams.bucket, [searchMetadata.searchInputFolder,searchMetadata.searchInputName].join('/'), tags);
            return {
                ...alignJobParams,
                completed: true,
                withErrors: false
            };
        } else if (job.status === 'FAILED') {
            const searchMetadata = getSearchMetadata(searchId);
            let errorMessage = searchMetadata.errorMessage;
            if (job.attempts && job.attempts.length > 0) {
                let reason = job.attempts[0].container && job.attempts[0].container.reason;
                if (reason) {
                    if (!!errorMessage) {
                        errorMessage = `${errorMessage}; ${reason}`;
                    } else {
                        errorMessage = reason;
                    }
                }
            }
            if (!errorMessage) {
                errorMessage = 'Alignment job failed';
            }
            await updateSearchMetadata({
                id: searchId,
                alignFinished: timestamp.toISOString(),
                errorMessage: errorMessage
            });
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
    console.log(`Fetching all keys in bucket ${bucket} with prefix ${intermediateSearchResultsPrefix}`)
    const allKeys = await getAllKeys({
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
        // we log the message but really not mark it as an error
        // this will allow the work to continue but final results may not include all partial results
        console.log(`Search timed out after ${elapsedSecs} seconds. Completed ${numComplete} of ${numBatches} jobs.`);
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
