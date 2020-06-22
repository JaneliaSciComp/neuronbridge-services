'use strict';

const utils = require('./utils');
const AWSXRay = require('aws-xray-sdk-core')
const AWS = require('aws-sdk');
// const AWS = AWSXRay.captureAWS(require('aws-sdk')
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


export const searchDispatch = async (event, context) => {
    
    const segment = AWSXRay.getSegment();
    var subsegment = segment.addNewSubsegment('Read parameters');

    // Parameters
    if (DEBUG) console.log(event);
    const level = event.level || 0;
    const numLevels = event.numLevels || 2;
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
        keys = keys.concat(await utils.getAllKeys(s3, { Bucket: libraryBucket, Prefix: library }));
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
    const partitions = utils.partition(keys, batchSize);
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
    await utils.putObject(s3, searchBucket, outputMetadataKey, searchMetadata);
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

        await utils.invokeAsync(lambda, searchFunction, searchParameters);
        console.log("Dispatched batch #", i++);
    }

    console.log("Parallel search started with output at", outputFolderUri);

    subsegment.close();

    if (stateMachineArn != null) {
        await startMonitor(outputKey, stateMachineArn, segment);
    }

    // Return the s3 bucket where the results will be saved
    return outputFolderUri;
}

const startMonitor = async (searchId, outputPrefix, stateMachineArn, segment) => {
    let subsegment = segment.addNewSubsegment('Start monitor');
    const monitorStateMachineInput = {
        bucket: searchBucket,
        prefix: outputPrefix
    };
    const stepFunction = new AWS.StepFunctions();
    const params = {
        stateMachineArn: stateMachineArn,
        input: JSON.stringify(monitorStateMachineInput),
        name: `ColorDepthSearch_${searchId}`
    };
    const result = await stepFunction.startExecution(params).promise();
    console.log("Step function started: ", result.executionArn);

    subsegment.close();

}