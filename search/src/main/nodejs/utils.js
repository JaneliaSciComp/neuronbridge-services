'use strict';

const AWS = require('aws-sdk');

AWS.config.apiVersions = {
    lambda: '2015-03-31',
};

const s3 = new AWS.S3();
const lambda = new AWS.Lambda();

const DEBUG = !!process.env.DEBUG;

exports.DEBUG = DEBUG;

// Retrieve all the keys in a particular bucket
exports.getAllKeys = async params => {
    const allKeys = [];
    var result;
    do {
        result = await s3.listObjectsV2(params).promise();
        result.Contents.forEach(obj => allKeys.push(obj.Key));
        params.ContinuationToken = result.NextContinuationToken;
    } while (result.NextContinuationToken);
    return allKeys;
};

// Retrieve a JSON file from S3
const getObject = async (bucket, key, defaultValue) => {
    try {
        if (DEBUG)
            console.log(`Getting object from ${bucket}:${key}`);
        const response = await s3.getObject({ Bucket: bucket, Key: key}).promise();
        const data = response.Body.toString();
        const jsonObject = JSON.parse(data);
        if (DEBUG)
            console.log(`Got object from ${bucket}:${key}:`, jsonObject);
        return jsonObject;
    } catch (e) {
        console.error(`Error getting object ${bucket}:${key}`, e);
        if (defaultValue === undefined) {
            throw e; // rethrow it
        } else {
            return defaultValue;
        }

    }
};

exports.getObject = getObject;

const sleep = async (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
}

exports.getObjectWithRetry = async (bucket, key, retries) => {
    for(let retry = 0; retry < retries; retry++) {
        try {
            return await getObject(bucket, key);
            await sleep(500);
        } catch (e) {
            if (retry + 1 >= retries) {
                console.error(`Error getting object ${bucket}:${key} after ${retries} retries`, e);
                throw e;
            }
        }
    }
}

// Retrieve a text file from S3
exports.getText = async (bucket, key) => {
    try {
        if (DEBUG)
            console.log(`Getting text from ${bucket}:${key}`);
        const response = await s3.getObject({ Bucket: bucket, Key: key}).promise();
        return response.Body.toString();
    } catch (e) {
        console.error(`Error getting object ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

// Write an object into S3 as JSON
exports.putObject = async (Bucket, Key, data) => {
    try {
        if (DEBUG)
            console.log(`Putting object to ${Bucket}:${Key}`);
        await s3.putObject({
            Bucket,
            Key,
            Body: JSON.stringify(data),
            ContentType: 'application/json'
        }).promise();
        if (DEBUG)
            console.log(`Put object to ${Bucket}:${Key}:`, data);
    } catch (e) {
        console.error('Error putting object', data, `to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`
};

// Write text to an S3 bucket
exports.putText = async (Bucket, Key, text) => {
    try {
        if (DEBUG)
            console.log(`Putting text to ${Bucket}:${Key}`);
        await s3.putObject({
            Bucket,
            Key,
            Body: text,
            ContentType: 'plain/text'
        }).promise();
        if (DEBUG)
            console.log(`Put text to ${Bucket}:${Key}:`, text);
    } catch (e) {
        console.error('Error putting object', text, `to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`
};

// Remove key from an S3 bucket
exports.removeKey = async (Bucket, Key) => {
    try {
        await s3.deleteObject({
            Bucket,
            Key}).promise();
        console.log(`DeleteObject ${Bucket}:${Key}`);
    } catch (e) {
        console.error(`Error deleting object ${Bucket}:${Key}`, e);
    }
};

// Returns consecutive sublists of a list, each of the same size (the final list may be smaller)
exports.partition = (list, size) => {
    const plist = [];
    for (var i = 0; i < list.length; i += size) {
        plist.push(list.slice(i, i + size));
    }
    return plist;
}

// Invoke another Lambda function
exports.invokeFunction = async (functionName, parameters) => {
    if (DEBUG)
        console.log(`Invoke function ${functionName} with`, parameters);
    const params = {
        FunctionName: functionName,
        InvocationType: 'Event',
        Payload: JSON.stringify(parameters),
    };
    try {
        return await lambda.invoke(params).promise();
    } catch (e) {
        console.error('Error invoking', params, e);
        throw e;
    }
}

// Invoke another Lambda function asynchronously
exports.invokeAsync = async (functionName, parameters) => {
    if (DEBUG)
        console.log(`Invoke async ${functionName} with`, parameters);
    const params = {
        FunctionName: functionName,
        InvokeArgs: JSON.stringify(parameters),
    };
    try {
        return await lambda.invokeAsync(params).promise();
    } catch (e) {
        console.error('Error invoking', params, e);
        throw e;
    }
}
