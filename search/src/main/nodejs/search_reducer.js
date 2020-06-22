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


export const searchReducer = async (event, context) => {

    // Parameters
    if (DEBUG) console.log(event);
    const bucket = event.bucket;
    const resultsPrefix = event.prefix;
    const numPartitions = event.numPartitions;

    const allMatches = [];
    for(let i = 0; i < numPartitions; i++) {
        const batchId = i.toString().padStart(4,"0");
        const outputKey = `${resultsPrefix}/batch_${batchId}.json`;
        const batchResults = await getObject(bucket, outputKey);
        // gather all results
        for(var j=0; j<batchResults.length; j++) {
            var maskMatches;
            if (i==0) {
                maskMatches = [];
                allMatches.push(maskMatches);
            } else {
                maskMatches = allMatches[j];
            }
            for(const result of batchResults[j]) {
                maskMatches.push(result);
            }
        }
    }

    // Sort the matches by descending score
    for(let i=0; i < allMatches.length; i++) {
        allMatches[i] = allMatches[i].sort((a, b) => b.score - a.score);
    }

    // Collate all matches to a file
    const outputUri = await putObject(bucket, prefix+"/results.json", allMatches);
    console.log(`Saved matches to ${outputUri}`);

}
