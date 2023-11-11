import { CognitoIdentityProviderClient, AdminGetUserCommand, ListUsersCommand } from "@aws-sdk/client-cognito-identity-provider";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { LambdaClient, InvokeCommand, LogType } from "@aws-sdk/client-lambda";
import { Upload } from "@aws-sdk/lib-storage";
import { S3Client, CopyObjectCommand, DeleteObjectCommand, GetObjectCommand,
         HeadObjectCommand, ListObjectsV2Command, PutObjectCommand } from "@aws-sdk/client-s3";
import { SFNClient, StartExecutionCommand } from "@aws-sdk/client-sfn";
import stream from 'stream';
import { backOff } from "exponential-backoff";

const s3Client = new S3Client();
const lambdaClient = new LambdaClient();
const stepFunctionClient = new SFNClient();
const cognitoISPClient = new CognitoIdentityProviderClient();
const dbDocClient = DynamoDBDocumentClient.from(new DynamoDBClient());

export const DEBUG = Boolean(process.env.DEBUG);

const retryOptions = {
    jitter : "full",
    maxDelay: 10000,
    startingDelay: 500,
    timeMultiple: 3,
    numOfAttempts: 3
};

export const sleep = async ms => {
    return new Promise(resolve => setTimeout(resolve, ms));
};

// Retrieve all the keys in a particular bucket
export const getAllKeys = async params => {
    const allKeys = [];
    let result;
    do {
        result = await s3Client.send(new ListObjectsV2Command(params));
        result.Contents.forEach(obj => allKeys.push(obj.Key));
        params.ContinuationToken = result.NextContinuationToken;
    } while (result.NextContinuationToken);
    result = null;
    return allKeys;
};

const getS3ContentAsString = async (bucket, key) => {
    try {
        if (DEBUG) console.log(`Getting content as string from ${bucket}:${key}`);
        const response = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key}));
        const bodyAsString = await response.Body.transformToString();
        return bodyAsString;
    } catch (e) {
        if (DEBUG) console.error(`Error getting content ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

// Retrieve a file from S3
export const getS3ContentAsStringWithRetry = async (bucket, key) => {
    return await backOff(() => getS3ContentAsString(bucket, key), {
        ...retryOptions,
        retry: (e, attemptNumber) => {
            console.error(`Failed attempt ${attemptNumber}/${retryOptions.numOfAttempts} getting object ${bucket}:${key}`, e);
            return true;
        }
    });
};

const getS3ContentAsByteArray = async (bucket, key) => {
    try {
        if (DEBUG) console.log(`Getting content as bytes from ${bucket}:${key}`);
        const response = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key}));
        const bodyAsArray = await response.Body.transformToByteArray();
        return bodyAsArray;
    } catch (e) {
        if (DEBUG) console.error(`Error getting content ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

export const getS3ContentAsByteArrayWithRetry = async (bucket, key) => {
    return await backOff(() => getS3ContentAsByteArray(bucket, key), {
        ...retryOptions,
        retry: (e, attemptNumber) => {
            console.error(`Failed attempt ${attemptNumber}/${retryOptions.numOfAttempts} getting object ${bucket}:${key}`, e);
            return true;
        }
    });
};

// Retrieve a JSON file from S3
export const getObjectWithRetry = async (bucket, key) => {
    const body = await getS3ContentAsStringWithRetry(bucket, key);
    return JSON.parse(body);
};

export const getS3ContentMetadata = async (bucket, key) => {
    try {
        if (DEBUG)
            console.log(`Getting content metadata for ${bucket}:${key}`);
        return await s3Client.send(new HeadObjectCommand({ Bucket: bucket, Key: key}));
    } catch (e) {
        console.error(`Error getting metadata for ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

export const putObjectWithRetry = async (bucket, key, data, space="\t") => {
    return await backOff(() => putObject(bucket, key, data, space), {
        ...retryOptions,
        retry: (e, attemptNumber) => {
            console.error(`Failed attempt ${attemptNumber}/${retryOptions.numOfAttempts} putting object ${bucket}:${key}`, e);
            return true;
        }
    });
};

// Write an object into S3 as JSON
export const putObject = async (Bucket, Key, data, space="\t") => {
    try {
        if (DEBUG)
            console.log(`Putting object to ${Bucket}:${Key}`);
        const res =  await s3Client.send(new PutObjectCommand({
            Bucket,
            Key,
            Body: JSON.stringify(data, null, space),
            ContentType: 'application/json'
        }));
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
        const res = await s3Client.send(new PutObjectCommand({
            Bucket,
            Key,
            Body: content,
            ContentType: contentType
        }));
        if (DEBUG) {
            console.log(`Put content to ${Bucket}:${Key}`, res);
        }
    } catch (e) {
        if (DEBUG) console.error('Error putting content', `to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`;
};

// Source is in the format '/${bucket}/${path}' and you probably want
// to encode it with encodeURI(), in case there are any invalid
// characters in there.
export const copyS3Content = async (Bucket, Source, Key) => {
    try {
        const params = {
            Bucket,
            CopySource: Source,
            Key,
        };
        if (DEBUG) {
            console.log(`Copying content to ${Bucket}:${Key} from ${Source} using:`, params);
        }

        const res = await s3Client.send(new CopyObjectCommand(params));
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
        const res = await s3Client.send(new DeleteObjectCommand({
            Bucket,
            Key,
        }));
        console.log(`Removed object ${Bucket}:${Key}`, res);
    } catch (e) {
        if (DEBUG) console.error(`Error removing object ${Bucket}:${Key}`, e);
        throw e; // rethrow it
    }
};

// Verify that key exists on S3
export const verifyKey = async (Bucket, Key) => {
    try {
        await s3Client.send(new HeadObjectCommand({Bucket, Key}));
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
        const upload = new Upload({
            client: s3Client,

            params: {
                Bucket,
                Key,
                Body: writeStream,
                ContentType: 'application/json'
            }
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
        await upload.done();
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
        InvocationType: 'RequestResponse',
        Payload: JSON.stringify(parameters),
        LogType: LogType.None,
    };
    try {
        return await lambdaClient.send(new InvokeCommand(params));
    } catch (e) {
        console.error(`Error invoking ${functionName}`, params, e);
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
    const result = await stepFunctionClient.send(new StartExecutionCommand(params));
    console.log("Step function started: ", result.executionArn);
    return result;
};

export const getOldSubs = async (username) => {
    const params = {
        UserPoolId: process.env.USERPOOL_ID,
        Username: username
    };
    // look up email address in the current user pool
    const user = await cognitoISPClient.send(new AdminGetUserCommand(params));
    const emailAddress = user.UserAttributes.find(e => e.Name === "email").Value;

    // find all users in the old user pool that have the matching
    // email address
    const old_pool_params = {
        UserPoolId: process.env.OLD_USERPOOL_ID,
        Filter: `email = "${emailAddress}"`
    };

    const usersRes = await cognitoISPClient.send(new ListUsersCommand(old_pool_params));

    let filteredUsers = [];

    if (/^Google_/.test(user.Username)) {
        // logged in with google
        filteredUsers = usersRes.Users.filter(userRes => /^Google_/.test(userRes.Username));
    } else {
        // logged in with cognito
        filteredUsers = usersRes.Users.filter(userRes => !/^Google_/.test(userRes.Username));
    }

    return filteredUsers.map(user => {
        return user.Username;
    });
};

//fetch data from original dynamodb table
async function getSearchRecords(ownerId, TableName) {
    const params = {
        TableName,
        FilterExpression: "#owner = :owner",
        ExpressionAttributeValues: {
            ":owner": ownerId
        },
        ExpressionAttributeNames: {
            "#owner": "owner"
        }
    };

    try {
        const data = await dbDocClient.send(new ScanCommand(params));
        if (data.Count > 0) {
            return data.Items;
        }
        return [];
    } catch (err) {
        console.log(err);
        return err;
    }
}

export const searchesToMigrate = async (username, oldUsernames) => {
    // check the ids in the new dynamo db table vs the old.
    // if old table contains any ids that are not in the new
    // table, then migration is required.
    const oldSearches = await getSearchRecords(
        oldUsernames[0],
        process.env.OLD_SEARCH_TABLE
    );
    console.log(oldSearches);
    const currentSearches = await getSearchRecords(username, process.env.SEARCH_TABLE);
    const currentLookup = currentSearches.map(search => search.id);
    const notMigrated = oldSearches.filter(
        search => !currentLookup.includes(search.id)
    );
    console.log(notMigrated);
    return notMigrated;
};

export const putDbItemWithRetry = async (tableName, item) => {
    return await backOff(() => putDbItem(tableName, item), {
        ...retryOptions,
        retry: (e, attemptNumber) => {
            console.error(`Failed attempt ${attemptNumber}/${retryOptions.numOfAttempts} to insert ${item} -> ${tableName}`, e);
            return true;
        }
    });
};

export const putDbItem = async (tableName, item) => {
    return await dbDocClient.send(new PutCommand({
        TableName: tableName,
        Item: item
    }));
};

export const getBucketNameFromURL = bucketURL => {
    const normalizedBucketURL = bucketURL.endsWith('/')
        ? bucketURL.slice(0, -1)
        : bucketURL;
    return normalizedBucketURL.substring(normalizedBucketURL.lastIndexOf('/') + 1);
};
