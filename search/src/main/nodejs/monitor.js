import AWS from 'aws-sdk';

import { getSearchMetadata, updateSearchMetadata} from './awsappsyncutils';

const bc = new AWS.Batch({
    apiVersion: '2016-08-10'
});

export const isJobDone = async (event) =>  {
    console.log('Input event:', JSON.stringify(event));
    try {
        return await monitorAlignmentJob(event);
    } catch (e) {
        console.log('Error while checking if job completed', event, e);
        await updateSearchMetadata({
            id: event.searchId,
            errorMessage: `Error while checking job status ${event.searchId} - ${e.message}`
        });
        return {
            ...event,
            completed: true,
            withErrors: true
        };
    }
};

const monitorAlignmentJob = async (alignJobParams) => {
    const searchId = alignJobParams.searchId;
    const jobId = alignJobParams.jobId;

    // retrieve jobs
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
                    if (errorMessage) {
                        errorMessage = `${errorMessage}; ${reason}`;
                    } else {
                        errorMessage = reason;
                    }
                }
            }
            if (!errorMessage) {
                errorMessage = 'Alignment job failed';
            }
            const timestamp = new Date();
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
};
