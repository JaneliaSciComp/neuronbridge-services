'use strict';

const DEBUG = false;

module.exports = {

    // Retrieve all the keys in a particular bucket
    getAllKeys: async function (s3, params) {
        const allKeys = [];
        var result;
        do {
            result = await s3.listObjectsV2(params).promise();
            result.Contents.forEach(obj => allKeys.push(obj.Key));
            params.ContinuationToken = result.NextContinuationToken;
        }
        while (result.NextContinuationToken);
        return allKeys;
    },

    // Parse a JSON file from S3
    getObject: async function (s3, bucket, key) {
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
    },

    // Write an object into S3 as JSON
    putObject: async function (s3, bucket, key, data) {
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
    },

    // Invoke another Lambda function asynchronously
    invokeAsync: async function (lambda, functionName, parameters) {
        const params = {
            FunctionName: functionName, 
            InvocationType: 'Event', // async invocation
            Payload: JSON.stringify(parameters),
        };
        return lambda.invoke(params).promise();
    },

    // Returns consecutive sublists of a list, each of the same size (the final list may be smaller)
    partition: function (list, size) {
        const output = [];
        for (var i = 0; i < list.length; i += size) {
            output[output.length] = list.slice(i, i + size);
        }
        return output;
    }
}