'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const lambda = new AWS.Lambda();

export const DEBUG = !!process.env.DEBUG;

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

// Retrieve a JSON file from S3
export const getObject = async (bucket, key) => {
    try {
        if (DEBUG) console.log(`Getting object from ${bucket}:${key}`);
        const response = await s3.getObject({ Bucket: bucket, Key: key}).promise();
        const data = response.Body.toString();
        if (DEBUG) console.log(`Got object from ${bucket}:${key}:`, data);
        return JSON.parse(data);
    } catch (e) {
        console.error(`Error getting object ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

// Retrieve a text file from S3
export const getText = async (bucket, key) => {
    try {
        if (DEBUG) console.log(`Getting text from ${bucket}:${key}`);
        const response = await s3.getObject({ Bucket: bucket, Key: key}).promise();
        return response.Body.toString();
    } catch (e) {
        console.error(`Error getting object ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

// Write an object into S3 as JSON
export const putObject = async (Bucket, Key, data) => {
    try {
        if (DEBUG) console.log(`Putting object to ${Bucket}:${Key}`);
        await s3.putObject({
            Bucket,
            Key,
            Body: JSON.stringify(data),
            ContentType: 'application/json'
        }).promise();
        if (DEBUG) console.log(`Put object to ${Bucket}:${Key}:`, data);
    } catch (e) {
        console.error('Error putting object', data, `to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`
};

// Write text to an S3 bucket
export const putText = async (Bucket, Key, text) => {
    try {
        if (DEBUG) console.log(`Putting text to ${Bucket}:${Key}`);
        await s3.putObject({
            Bucket,
            Key,
            Body: text,
            ContentType: 'plain/text'
        }).promise();
        if (DEBUG) console.log(`Put text to ${Bucket}:${Key}:`, text);
    } catch (e) {
        console.error('Error putting object', text, `to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`
};

// Remove key from an S3 bucket
export const removeKey = async (Bucket, Key) => {
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
export const partition = (list, size) => {
    const plist = [];
    for (var i = 0; i < list.length; i += size) {
        plist.push(list.slice(i, i + size));
    }
    return plist;
};

// Invoke another Lambda function asynchronously
export const invokeAsync = async (functionName, parameters) => {
    const params = {
        FunctionName: functionName,
        InvocationType: 'Event', // async invocation
        Payload: JSON.stringify(parameters),
    };
    return lambda.invoke(params).promise();
};
