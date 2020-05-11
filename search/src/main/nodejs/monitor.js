'use strict';
const AWS = require('aws-sdk');
const moment = require('moment');

const s3 = new AWS.S3();
const DEBUG = false;

// From https://stackoverflow.com/questions/42394429/aws-sdk-s3-best-way-to-list-all-keys-with-listobjectsv2
async function getAllKeys(params,  allKeys = []) {
    const response = await s3.listObjectsV2(params).promise();
    response.Contents.forEach(obj => allKeys.push(obj.Key));
    if (response.NextContinuationToken) {
        params.ContinuationToken = response.NextContinuationToken;
        await getAllKeys(params, allKeys); // RECURSIVE CALL
    }
    return allKeys;
}

// Read a JSON file from S3 and parse it into an object
async function getObject(bucket, key) {
    try {
        if (DEBUG) console.log(`Getting object from ${bucket}:${key}`);
        const response = await s3.getObject({ Bucket: bucket, Key: key}).promise();
        const data = response.Body.toString();
        if (DEBUG) console.log(`Got object from ${bucket}:${key}:`, data);
        return JSON.parse(data);
    } 
    catch (e) {
        console.error(`Error getting object ${bucket}:${key}`, e);
        throw e;
    }
}

async function putObject(bucket, key, data) {
    try {
        if (DEBUG) console.log(`Putting object to ${bucket}:${key}`);
        const body = JSON.stringify(data);
        await s3.putObject({ Bucket: bucket, Key: key, Body: body, ContentType: 'application/json'}).promise();
        if (DEBUG) console.log(`Put object to ${bucket}:${key}:`, data);
    } 
    catch (e) {
        console.error('Error putting object', data, `to ${bucket}:${key}`, e);
        throw e;
    }
}

exports.isSearchDone = async (event, context) => {

    // Parameters
    console.log(event);
    const bucket = event.bucket;
    const prefix = event.prefix;

    if (bucket == null) {
        throw new Error('Missing required key \'bucket\' in input to isSearchDone');
    }

    if (prefix == null) {
        throw new Error('Missing required key \'prefix\' in input to isSearchDone');
    }

    // Fetch metadata
    const metadata = await getObject(bucket, prefix+"/metadata.json");
    const numPartitions = metadata['partitions'];
    const startTime = moment(metadata['startTime']);

    // Fetch all keys
    const allKeys = new Set(await getAllKeys({ Bucket: bucket, Prefix: prefix }));
    if (DEBUG) console.log(allKeys);

    // Check if all partitions have completed
    let numComplete = 0;
    for(let i=0; i<numPartitions; i++) {
        const batchId = i.toString().padStart(4,"0");
        const outputKey = prefix+"/batch_"+batchId+".json";
        if (allKeys.has(outputKey)) {
            numComplete++;
        }
    }

    const completed = numPartitions === numComplete;
    const numRemaining = numPartitions - numComplete;

    if (completed) {
        console.log(`Search complete: ${numRemaining}/${numPartitions}`);

        const now = new Date();
        const endTime = moment(now.toISOString());
        const elapsed = endTime.diff(startTime, "s");
        console.log(`Search took ${elapsed} seconds`);

        // Combine all the results
        const allMatches = [];
        for(let i=0; i<numPartitions; i++) {
            const batchId = i.toString().padStart(4,"0");
            const outputKey = prefix+"/batch_"+batchId+".json";
            const batch = await getObject(bucket, outputKey);
            for(const match of batch) {
                allMatches.push(match);
            }
        }
        // Sort the matches by descending score
        const sortedMatches = allMatches.sort((a, b) => b.score - a.score);
        // Collate all matches to a file
        await putObject(bucket, prefix+"/results.json", sortedMatches);

        // TODO: in the future this might write to a WebSocket to notify the user that the search is complete
        // Return results which Step Functions will use to determine if this monitor should run again
        return {
            elapsedSecs: elapsed,
            numPartitions: numPartitions,
            completed: true
        };
    }
    else {
        console.log(`Search still running: ${numComplete}/${numPartitions}`);
        // Return results which Step Functions will use to determine if this monitor should run again
        return {
            ...event,
            numPartitions: numPartitions,
            numRemaining: numRemaining,
            completed: false
        };
    }

}