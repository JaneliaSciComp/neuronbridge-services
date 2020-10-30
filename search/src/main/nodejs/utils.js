import AWS from 'aws-sdk';
import stream from 'stream';
import { backOff } from "exponential-backoff";

AWS.config.apiVersions = {
    lambda: '2015-03-31',
    s3: '2006-03-01',
};

const s3 = new AWS.S3();
const lambda = new AWS.Lambda();
const stepFunction = new AWS.StepFunctions();

export const DEBUG = Boolean(process.env.DEBUG);

const retryOptions = {
    jitter : "full",
    maxDelay: 10000,
    startingDelay: 200,
    numOfAttempts: 3
};

export const sleep = async (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
};

// Retrieve all the keys in a particular bucket
export const getAllKeys = async params => {
    const allKeys = [];
    var result;
    do {
        result = await s3.listObjectsV2(params).promise();
        result.Contents.forEach(obj => allKeys.push(obj.Key));
        params.ContinuationToken = result.NextContinuationToken;
    } while (result.NextContinuationToken);
    return allKeys;
};

const getS3Content = async (bucket, key) => {
    try {
        if (DEBUG)
            console.log(`Getting content from ${bucket}:${key}`);
        const response = await s3.getObject({ Bucket: bucket, Key: key}).promise();
        return response.Body;
    } catch (e) {
        if (DEBUG) console.error(`Error getting content ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

// Retrieve a file from S3
export const getS3ContentWithRetry = async (bucket, key) => {
    return await backOff(() => getS3Content(bucket, key), {
        ...retryOptions,
        retry: (e, attemptNumber) => {
            console.error(`Failed attempt ${attemptNumber}/${retryOptions.numOfAttempts} getting object ${bucket}:${key}`, e);
            return true;
        }
    });
};

export const getObjectDataArray = async (bucket, key) => {
    try {
        const s3Content = await getS3ContentWithRetry(bucket, key);
        return s3Content.buffer;
    } catch (e) {
        if (DEBUG) console.error(`Error getting object data array from ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

// Retrieve a JSON file from S3
export const getObjectWithRetry = async (bucket, key) => {
    const body = await getS3ContentWithRetry(bucket, key);
    return JSON.parse(body.toString());
};

export const getS3ContentMetadata = async (bucket, key) => {
    try {
        if (DEBUG)
            console.log(`Getting content metadata for ${bucket}:${key}`);
        return await s3.headObject({ Bucket: bucket, Key: key}).promise();
    } catch (e) {
        console.error(`Error getting metadata for ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

export const putObjectWithRetry = async (bucket, key, data) => {
    return await backOff(() => putObject(bucket, key, data), {
        ...retryOptions,
        retry: (e, attemptNumber) => {
            console.error(`Failed attempt ${attemptNumber}/${retryOptions.numOfAttempts} putting object ${bucket}:${key}`, e);
            return true;
        }
    });
};

// Write an object into S3 as JSON
export const putObject = async (Bucket, Key, data) => {
    try {
        if (DEBUG)
            console.log(`Putting object to ${Bucket}:${Key}`);
        const res =  await s3.putObject({
            Bucket,
            Key,
            Body: JSON.stringify(data, null , "\t"),
            ContentType: 'application/json'
        }).promise();
        if (DEBUG) {
            console.log(`Put object to ${Bucket}:${Key}:`, data, res);
        }
    } catch (e) {
        console.error('Error putting object', data, `to ${Bucket}:${Key}`, e);
        throw e; // rethrow it
    }
    return `s3://${Bucket}/${Key}`;
};

// Write content to an S3 bucket
export const putS3Content = async (Bucket, Key, contentType, content) => {
    try {
        if (DEBUG) {
            console.log(`Putting content to ${Bucket}:${Key}`);
        }
        const res = await s3.putObject({
            Bucket,
            Key,
            Body: content,
            ContentType: contentType
        }).promise();
        if (DEBUG) {
            console.log(`Put content to ${Bucket}:${Key}`, res);
        }
    } catch (e) {
        if (DEBUG) console.error('Error putting content', `to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`;
};

export const copyS3Content = async (Bucket, Source, Key) => {
   try {
        if (DEBUG) {
            console.log(`Copying content to ${Bucket}:${Key} from ${Source}`);
        }
        const res = await s3.copyObject({
            Bucket,
            CopySource: Source,
            Key,
        }).promise();
        if (DEBUG) {
            console.log(`Copied content to ${Bucket}:${Key} from ${Source}`, res);
        }
    } catch (e) {
        console.error(`Error copying content to ${Bucket}:${Key} from ${Source}`, e);
        throw e; // rethrow it
    }
    return `s3://${Bucket}/${Key}`;

};

// Remove key from an S3 bucket
export const removeKey = async (Bucket, Key) => {
    try {
        const res = await s3.deleteObject({
            Bucket,
            Key}).promise();
        console.log(`Removed object ${Bucket}:${Key}`, res);
    } catch (e) {
        if (DEBUG) console.error(`Error removing object ${Bucket}:${Key}`, e);
        throw e; // rethrow it
    }
};

// Verify that key exists on S3
export const verifyKey = async (Bucket, Key) => {
    try {
        await s3.headObject({Bucket, Key}).promise();
        console.log(`Found object ${Bucket}:${Key}`);
        return true;
    } catch (e) {
        console.error(`Error looking up ${Bucket}:${Key}`, e);
        return false;
    }
};

export const streamObject = async (Bucket, Key, data) => {
    try {
        console.log(`Streaming object to ${Bucket}:${Key}`);
        const writeStream = new stream.PassThrough();
        const uploadPromise = s3.upload({
            Bucket,
            Key,
            Body: writeStream,
            ContentType: 'application/json'
        });

        const dataStream = new stream.Readable({objectMode: true});
        dataStream.pipe(writeStream);
        dataStream.on('end', () => {
            console.log('Finished writing the data stream');
        });
        // json serialiaze the data
        dataStream.push('{\n');
        Object.entries(data).forEach(([key, value],  index) => {
            if (index > 0)  {
                dataStream.push(',\n');
            }
            dataStream.push(`"${key}": `);
            if (Array.isArray(value)) {
                dataStream.push('[');
                value.forEach((arrayElem, arrayIndex) => {
                    if (arrayIndex > 0) {
                        dataStream.push(',\n');
                    }
                    dataStream.push(JSON.stringify(arrayElem));
                });
                dataStream.push(']');
            } else {
                dataStream.push(JSON.stringify(value));
            }
        });
        dataStream.push('\n}');
        dataStream.push(null);
        await uploadPromise.promise();
        console.log(`Finished streaming data to ${Bucket}:${Key}`);
    } catch (e) {
        console.error(`Error streaming data to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`;
};

// Returns consecutive sublists of a list, each of the same size (the final list may be smaller)
export const partition = (list, size) => {
    // If the size was passed in as a string, concatenation would happen instead of addition.
    const sublistSize = parseInt(size);
    const plist = [];
    for (var i = 0; i < list.length; i += sublistSize) {
        let arr = list.slice(i, i + sublistSize);
        plist.push(arr);
    }
    return plist;
};

// Invoke another Lambda function
export const invokeFunction = async (functionName, parameters) => {
    if (DEBUG)
        console.log(`Invoke sync ${functionName} with`, parameters);
    const params = {
        FunctionName: functionName,
        Payload: JSON.stringify(parameters),
        LogType: "Tail"
    };
    try {
        return await lambda.invoke(params).promise();
    } catch (e) {
        console.error(`Error invoking ${functionName}`, params, e);
        throw e; // rethrow it
    }
};

// Invoke another Lambda function asynchronously
export const invokeAsync = async (functionName, parameters) => {
    if (DEBUG)
        console.log(`Invoke async ${functionName} with`, parameters);
    const params = {
        FunctionName: functionName,
        InvokeArgs: JSON.stringify(parameters),
    };
    try {
        return await lambda.invokeAsync(params).promise();
    } catch (e) {
        console.error(`Error invoking async ${functionName}`, params, e);
        throw e; // rethrow it
    }
};

// Start state machine
export const startStepFunction = async (uniqueName, stateMachineParams, stateMachineArn) => {
    const params = {
        stateMachineArn: stateMachineArn,
        input: JSON.stringify(stateMachineParams),
        name: uniqueName
    };
    const result = await stepFunction.startExecution(params).promise();
    console.log("Step function started: ", result.executionArn);
    return result;
};
