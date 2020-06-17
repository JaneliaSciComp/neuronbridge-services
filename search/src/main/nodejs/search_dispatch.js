'use strict';

const utils = require('./utils');
const AWSXRay = require('aws-xray-sdk-core')
const AWS = require('aws-sdk');
// const AWS = AWSXRay.captureAWS(require('aws-sdk')
const { v1: uuidv1 } = require('uuid');

const DEBUG = true;
const s3 = new AWS.S3();
//const lambda = AWSXRay.captureAWSClient(new AWS.Lambda());
const lambda = new AWS.Lambda();

const MAX_PARALLELISM = 3000;

const DEFAULTS = {
    level: 0,
    numLevels: 2,
    batchSize: 40,
    dataThreshold: 100,
    pixColorFluctuation: 2.0,
    xyShift: 0,
    mirrorMask: false,
    minMatchingPix: 5,
};

const region = process.env.AWS_REGION;
const maskBucket = process.env.MASK_BUCKET;
const libraryBucket = process.env.LIBRARY_BUCKET;
const searchBucket = process.env.SEARCH_BUCKET;
const dispatchFunction = process.env.DISPATCH_FUNCTION;
const searchFunction = process.env.SEARCH_FUNCTION;
const stateMachineArn = process.env.STATE_MACHINE_ARN;

async function getCount(library) {
    const countMetadata = await utils.getObject(s3, libraryBucket, library+"/counts_denormalized.json");
    console.log("Retrieved count metadata: ", countMetadata);
    return countMetadata.objectCount;
    //return 1337;
}

async function getKeys(library) {
    const keys = await utils.getObject(s3, libraryBucket, library+"/keys_denormalized.json");
    return keys;
    //return keys.slice(0, 1337);
}

exports.searchDispatch = async (event, context) => {

    const segment = AWSXRay.getSegment();
    var subsegment = segment.addNewSubsegment('Read parameters');

    if (DEBUG) console.log(event);
    
    const username = "anonymous";

    // Parameters without defaults; must be specified by the user
    const library = event.library;
    const maskKeys = event.maskKeys;
    const maskThresholds = event.maskThresholds;

    if (library == null) {
        throw new Error('Missing required key \'library\' in input');
    }

    if (maskKeys == null) {
        throw new Error('Missing required key \'maskKeys\' in input');
    }

    if (maskKeys.length != maskThresholds.length) {
        throw new Error('Number of mask thresholds does not match number of masks');
    }

    // Parameters which have defaults
    const level = event.level || DEFAULTS.level;
    const numLevels = event.numLevels || DEFAULTS.numLevels;
    const dataThreshold = event.dataThreshold || DEFAULTS.dataThreshold;
    const pixColorFluctuation = event.pixColorFluctuation || DEFAULTS.pixColorFluctuation;
    const xyShift = event.xyShift || DEFAULTS.xyShift;
    const mirrorMask = event.mirrorMask || DEFAULTS.mirrorMask;
    const minMatchingPix = event.minMatchingPix || DEFAULTS.minMatchingPix;

    // Programmatic parameters. In the case of the root manager, these will be null initially and then generated for later invocations.
    let searchUid = event.searchUid;
    let outputFolder = event.outputFolder;
    let outputFolderUri = event.outputFolderUri;
    let batchSize = event.batchSize || DEFAULTS.batchSize;
    let numBatches = event.numBatches;
    let startIndex = event.startIndex;
    let endIndex = event.endIndex;

    const nextEvent = {
        level: level + 1,
        numLevels: numLevels,
        library: library,
        maskKeys: maskKeys,
        dataThreshold: dataThreshold,
        maskThresholds: maskThresholds,
        pixColorFluctuation: pixColorFluctuation,
        xyShift: xyShift,
        mirrorMask: mirrorMask,
        minMatchingPix: minMatchingPix,
        searchUid: searchUid,
        outputFolder: outputFolder,
        outputFolderUri: outputFolderUri,
        batchSize: batchSize,
        numBatches: numBatches
    };

    if (level+1 < numLevels) {

        subsegment.close();
        subsegment = segment.addNewSubsegment('Subdivide');

        // Dispatch subdivided ranges

        if (level==0) {
            // This is the top-level manager, so we need to find our range first
            const total = await getCount(library);
            // Calculate batch size, capping at a max level of parallelism
            nextEvent.numBatches = numBatches = Math.ceil(total / batchSize);
            if (numBatches > MAX_PARALLELISM) {
                nextEvent.batchSize = batchSize = Math.ceil(total / MAX_PARALLELISM);
                nextEvent.numBatches = numBatches = Math.ceil(total / batchSize);
                console.log(`Capping batch size to ${batchSize} due to max parallelism (${MAX_PARALLELISM})`);
            }
            
            startIndex = 0;
            endIndex = total;
        }
        
        const count = endIndex - startIndex; // e.g. 34717

        console.log("Batch size:", batchSize);
        console.log("Num batches:", numBatches);
        console.log(`Item range: ${startIndex} - ${endIndex} (${count} items)`);

        // How much to branch at each manager level in order to accomodate numLevels of managers
        const branchingFactor = Math.ceil(Math.pow(numBatches, 1/numLevels)); // e.g. ceil(695^(1/3)) = ceil(8.86) = 9
        console.log(`numNextLevelManagers: ${branchingFactor}`);

        // How many items each manager gets at the next level
        const nextLevelManagerRange = Math.pow(branchingFactor, numLevels-level-1) * batchSize;
        console.log(`nextLevelManagerRange: ${nextLevelManagerRange}`);

        if (level==0) {
            // Also, save metadata about this search

            subsegment.close();
            subsegment = segment.addNewSubsegment('Persist metadata');

            nextEvent.searchUid = searchUid = uuidv1();
            nextEvent.outputFolder = outputFolder = `${username}/${searchUid}`;
            nextEvent.outputFolderUri = outputFolderUri = `s3://${searchBucket}/${outputFolder}`;
            
            const outputMetadataKey = `${outputFolder}/metadata.json`;
            const outputMetadataUri = `s3://${searchBucket}/${outputMetadataKey}`;
            const now = new Date();
            const searchMetadata = {
                startTime: now.toISOString(),
                parameters: event,
                itemCount: count,
                partitions: numBatches
            };
            await utils.putObject(s3, searchBucket, outputMetadataKey, searchMetadata);
            console.log("Parallel search started with output at", outputFolderUri);
            console.log(`Metadata written to ${outputMetadataUri}`);
        } 

        subsegment.close();
        subsegment = segment.addNewSubsegment('Launch sub-managers');

        // Dispatch all sub workers
        var j = 0;
        for(let i = startIndex; i < endIndex; i += nextLevelManagerRange) {
            const workerStart = i;
            const workerEnd = i+nextLevelManagerRange > endIndex ? endIndex : i+nextLevelManagerRange;
            await utils.invokeAsync(lambda, dispatchFunction, {startIndex:workerStart, endIndex:workerEnd, ...nextEvent});
            console.log(`Dispatched sub-manager #${j} (${workerStart} - ${workerEnd})`);
            j++;
        }

        subsegment.close();

        if (level==0) {
            // Start monitoring using the step function
            subsegment = segment.addNewSubsegment('Start search monitor');

            if (stateMachineArn!=null) {
                const monitorStateMachineInput = {
                    bucket: searchBucket,
                    prefix: outputFolder
                };
                const stepFunction = new AWS.StepFunctions();
                const params = {
                    stateMachineArn: stateMachineArn,
                    input: JSON.stringify(monitorStateMachineInput),
                    name: "ColorDepthSearch_"+searchUid
                };
                const result = await stepFunction.startExecution(params).promise();
                console.log("Search monitor started: ", result.executionArn);
            }

            subsegment.close();
        }

    }
    else {
        // We're in a leaf manager, dispatch the workers
        console.log(`Item range: ${startIndex}-${endIndex}`);

        subsegment.close();
        subsegment = segment.addNewSubsegment('Get library keys');
        
        const allKeys = await getKeys(library)
        const keys = allKeys.slice(startIndex, endIndex);
        console.log("Total number of images to search: "+keys.length);
    
        subsegment.close();
        subsegment = segment.addNewSubsegment('Execute batches');
    
        // Dispatch all batches
        var i = Math.ceil(startIndex / batchSize);
        const partitions = utils.partition(keys, batchSize);

        for (const searchKeys of partitions) {
    
            const batchId = i.toString().padStart(4,"0");
            const outputFile = `${outputFolderUri}/batch_${batchId}.json`;
    
            const searchParameters = {
                searchPrefix: libraryBucket,
                searchKeys: searchKeys,
                maskPrefix: maskBucket,
                maskKeys: maskKeys,
                outputFile: outputFile,
            };

            await utils.invokeAsync(lambda, searchFunction, {...searchParameters, ...nextEvent});
            console.log(`Dispatched batch #${i} (${searchKeys.length} items)`);
            i++;
        }
        
        subsegment.close();
    }

    // Return the s3 bucket where the results will be saved
    return outputFolderUri;
}