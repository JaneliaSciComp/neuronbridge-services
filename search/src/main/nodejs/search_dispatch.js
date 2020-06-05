'use strict';
const AWS = require('aws-sdk');
const AWSXRay = require('aws-xray-sdk');
const { v1: uuidv1 } = require('uuid');

const s3 = new AWS.S3();
const DEBUG = false;

const DEFAULT_BATCH_SIZE = 50;
const MAX_PARALLELISM = 1000;
const DEFAULT_DATA_THRESHOLD = 100;
const DEFAULT_PIXCOLORFLUCUTATION = 2.0;
const DEFAULT_XYSHIFT = 0;
const DEFAULT_MIRRORMASK = false;
const DEFAULT_MINMAXPIX = 5;

const region = process.env.AWS_REGION;
const maskBucket = process.env.MASK_BUCKET;
const libraryBucket = process.env.LIBRARY_BUCKET;
const searchBucket = process.env.SEARCH_BUCKET;
const dispatchFunction = process.env.DISPATCH_FUNCTION;
const searchFunction = process.env.SEARCH_FUNCTION;
const stateMachineArn = process.env.STATE_MACHINE_ARN;

// From https://stackoverflow.com/questions/42394429/aws-sdk-s3-best-way-to-list-all-keys-with-listobjectsv2
async function getAllKeys(params,  allKeys = []) {
    const response = await s3.listObjectsV2(params).promise();
    response.Contents.forEach(obj => allKeys.push(obj.Key));
    if (response.NextContinuationToken) {
        params.ContinuationToken = response.NextContinuationToken;
        await getAllKeys(params, allKeys); // recursive call
    }
    return allKeys;
}

// Parse a JSON file from S3
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

// Write an object into S3 as JSON
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

// Returns consecutive sublists of a list, each of the same size (the final list may be smaller)
function partition(list, size) {
    const output = [];
    for (var i = 0; i < list.length; i += size) {
        output[output.length] = list.slice(i, i + size);
    }
    return output;
}

exports.searchDispatch = async (event, context) => {
    
    const segment = AWSXRay.getSegment(); 
    var subsegment = segment.addNewSubsegment('Read parameters');

    // Parameters
    console.log(event);
    const level = event.level || 0;
    const numLevels = event.numLevels || 1;
    const batchSize = event.batchSize || DEFAULT_BATCH_SIZE;
    const libraries = event.libraries;
    const maskKeys = event.maskKeys;
    const maskThresholds = event.maskThresholds;

    if (libraries == null) {
        throw new Error('Missing required key \'libraries\' in input');
    }

    if (maskKeys == null) {
        throw new Error('Missing required key \'maskKeys\' in input');
    }

    if (maskKeys.length != maskThresholds.length) {
        throw new Error('Number of mask thresholds does not match number of masks');
    }

    subsegment.close();
    subsegment = segment.addNewSubsegment('Get library keys');

    var keys = [];
    for(const library of libraries) {
        console.log(`Finding images in library bucket ${libraryBucket} with prefix ${library}`);
        keys = keys.concat(await getAllKeys({ Bucket: libraryBucket, Prefix: library }));
    }
    console.log("Total number of images to search: "+keys.length);

    subsegment.close();
    subsegment = segment.addNewSubsegment('Calculate partitions');

    // Calculate batch size, capping at a max level of parallelism
    const total = keys.length;
    const numBatches = total / batchSize;

    if (numBatches > MAX_PARALLELISM) {
        batchSize = total / MAX_PARALLELISM;
    }
    console.log("Batch size:", batchSize);

    // Create partitions
    const partitions = partition(keys, batchSize);
    console.log("Num partitions:", partitions.length);

    const uid = uuidv1();
    const numPartitions = partitions.length;

    subsegment.close();
    subsegment = segment.addNewSubsegment('Persist metadata');

    const username = "anonymous";
    const outputKey = `${username}/${uid}`;
    const outputFolderUri = `s3://${searchBucket}/${outputKey}`;
    const outputMetadataKey = `${outputKey}/metadata.json`;
    const outputMetadataUri = `s3://${searchBucket}}/${outputMetadataKey}`;
    const now = new Date();

    const searchMetadata = {
        startTime: now.toISOString(),
        parameters: event,
        partitions: numPartitions
    };
    await putObject(searchBucket, outputMetadataKey, searchMetadata);
    console.log(`Metadata written to ${outputMetadataUri}`);

    subsegment.close();
    subsegment = segment.addNewSubsegment('Execute batches');

    // Dispatch all batches
    var lambda = new AWS.Lambda();
    var i = 0;
    for (const batchKeys of partitions) {

        if (i>=numPartitions) break;
        const batchId = i.toString().padStart(4,"0");
        const outputFile = `${outputFolderUri}/batch_${batchId}.json`;

        const searchParameters = {
            searchPrefix: libraryBucket,
            searchKeys: batchKeys,
            maskPrefix: maskBucket,
            maskKeys: maskKeys,
            dataThreshold: event.dataThreshold || DEFAULT_DATA_THRESHOLD,
            maskThresholds: maskThresholds,
            pixColorFluctuation: event.pixColorFluctuation || DEFAULT_PIXCOLORFLUCUTATION,
            xyShift: event.xyShift || DEFAULT_XYSHIFT,
            mirrorMask: event.mirrorMask || DEFAULT_MIRRORMASK,
            outputFile: outputFile,
            minMatchingPix: event.minMatchingPix || DEFAULT_MINMAXPIX
        };

        const params = {
            FunctionName: searchFunction, 
            InvocationType: 'Event', // async invocation
            Payload: JSON.stringify(searchParameters)
        };
        await lambda.invoke(params).promise();
        
        console.log("Dispatched batch #", i++);
    }

    console.log("Parallel search started with output at", outputFolderUri);

    subsegment.close();
    subsegment = segment.addNewSubsegment('Start monitor');

    if (stateMachineArn!=null) {
        const monitorStateMachineInput = {
            bucket: searchBucket,
            prefix: outputKey
        };
        const stepFunction = new AWS.StepFunctions();
        const params = {
            stateMachineArn: stateMachineArn,
            input: JSON.stringify(monitorStateMachineInput),
            name: "ColorDepthSearch_"+uid
          };
        const result = await stepFunction.startExecution(params).promise();
        console.log("Step function started: ", result.executionArn);
    }

    subsegment.close();

    // Return the s3 bucket where the results will be saved
    return outputFolderUri;
}