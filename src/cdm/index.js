'use strict';

const AWS = require('aws-sdk');
const path = require('path');

// aws clients
const s3 = new AWS.S3();
const bc = new AWS.Batch();

const jobDefinition = process.env.JOB_DEFINITION;
const jobQueue = process.env.JOB_QUEUE;
const templatesBucket = process.env.TEMPLATES_BUCKET;
const outputsBucket = process.env.OUTPUTS_BUCKET;
const debugFlag = process.env.DEBUG;
const nslots = '16';
const iamRole = 'auto';

exports.generateColorDepthMips = async (event) => {
    const eventRecords = event.Records;
    console.log('Notification received for', eventRecords);
    await eventRecords.reduce(async (promise, eventRecord) => {
        // This line will wait for the last async function to finish.
        // The first iteration uses an already resolved Promise
        // so, it will immediately continue.
        await promise;
        await processS3EventNotification(eventRecord)
      }, Promise.resolve());
}

const processS3EventNotification = async (notif) => {
    const inputsBucket = notif.s3.bucket.name;
    const inputFilepath = decodeURIComponent(notif.s3.object.key);
    const inputFileName = path.basename(inputFilepath.replace(/\+/g, ' '));

    try {
        console.log(`Process notification ${inputsBucket}:${inputFilepath} (${inputFileName}) `);
        const metadata = await readDataAsJson(inputsBucket, inputFilepath);

        var fnAndExtMatch = inputFilepath.match(/(.+)\.([^.]*)$/);
        if (!fnAndExtMatch) {
            console.error(`Could not determine the filename and file extension from ${inputsBucket}:${inputFilepath}`);
            return;
        }
        var inputFileWithNoExt = fnAndExtMatch[1];

        const inputsBucketContent = await s3.listObjects({
            Bucket: inputsBucket,
            Prefix: inputFileWithNoExt
        }).promise();

        console.log(`${inputsBucket} Bucket Content:`, inputsBucketContent);
        const inputImageEntry = inputsBucketContent.Contents.find(c => c.Key != inputFilepath);
        if (!inputImageEntry) {
            console.error(`Could not find the corresponding image file for ${inputFilepath} in ${inputsBucket}`);
            return;
        } else {
            console.log(`Found image file ${inputsBucket}`, inputImageEntry);
        }

        const inputImageFilename = inputImageEntry.Key;

        console.log('Job metadata', metadata);
        const user = metadata.meta['user'];
        const sampleName = metadata.meta['sample'].replace(/ /g, '_');
        const gender = metadata.meta['gender'];
        const area = metadata.meta['area'];
        const shape = 'Unknown';
        const objective = metadata.meta['objective'];
        const mountingProtocol = metadata.meta['mounting protocol'];
        const imageSize = metadata.meta['image size'];
        const voxelSize = metadata.meta['voxel size'];
        const nchannels = metadata.meta['channels'];
        const refChannel = metadata.meta['reference channel'];

        const jobResources = {
            'vcpus': 16,
            'memory': 8192
        };
        const jobName = `align-${user}-${sampleName}-${area}-${gender}-${objective}`;
        const jobParameters = {
            'gender': gender,
            'area': area,
            'shape': shape,
            'objective': objective,
            'mounting_protocol': mountingProtocol,
            'image_size': `${imageSize.x}x${imageSize.y}x${imageSize.z}`,
            'voxel_size': `${voxelSize.x}x${voxelSize.y}x${voxelSize.z}`,
            'nchannels': nchannels,
            'reference_channel': refChannel,
            'templates_bucket': templatesBucket,
            'inputs_bucket': inputsBucket,
            'outputs_bucket': outputsBucket,
            'input_filename': `/${inputImageFilename}`,
            'output_folder': `/${user}/${gender}/${area}/${objective}/${sampleName}`,
            'nslots': nslots,
            'iam_role': iamRole,
            'debug_flag': debugFlag
        };

        const params = {
            'jobDefinition': jobDefinition,
            'jobQueue': jobQueue,
            'jobName': jobName,
            'containerOverrides': jobResources,
            'parameters': jobParameters
        };

        // submit batch job
        console.log('Job parameters', params);
        const job = await bc.submitJob(params).promise();
        console.log('Submitted', job);
        console.log(`Job ${job.jobName} launched with id ${job.jobId}`, job);

        // move the metadata file to the output bucket
        await writeDataAsJson (outputsBucket, `/${user}/${sampleName}.json`, metadata);
        await deleteData(inputsBucket, inputFilepath);
    } catch (e) {
        console.error(`Error processing ${inputsBucket}:${inputFilepath}`, e);
        throw new Error(e);
    }
}

const readDataAsJson = async (srcBucket, srcKey) => {
    try {
        const data = await s3.getObject({ 
            Bucket: srcBucket,
            Key: srcKey }).promise();
        const json = data.Body.toString();
        console.log("GetObject data", json);
        return JSON.parse(json);
    } catch (e) {
        console.error(`Error getting object ${srcBucket}:${srcKey}`, e);
        throw e; // rethrow it
    }
}

const deleteData = async (Bucket, Key) => {
    try {
        await s3.deleteObject({
            Bucket,
            Key}).promise();
        console.log(`DeleteObject ${Bucket}:${Key}`);
    } catch (e) {
        console.error(`Error deleting object ${Bucket}:${Key}`, e);
    }
}

const writeDataAsJson = async (Bucket, Key, data) => {
    try {
        await s3.putObject({
            Bucket,
            Key,
            Body: JSON.stringify(data),
            ContentType: 'application/json'}).promise();
        console.log(`PutObject ${Bucket}:${Key}`, data);
    } catch (e) {
        console.error('Error writing object', data, `to ${Bucket}:${Key}`, e);
    }
}
