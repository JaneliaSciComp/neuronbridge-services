'use strict';

const AWS = require('aws-sdk');
const {getSearchMaskId, getSearchSubFolder} = require('./searchutils');
const {updateSearchMetadata, ALIGNMENT_JOB_COMPLETED} = require('./awsappsyncutils');

const s3 = new AWS.S3();

const searchBucket = process.env.SEARCH_BUCKET;
const TEST_IMAGE_BUCKET = process.env.SEARCH_BUCKET;
const TEST_IMAGE = 'colorDepthTestData/test1/mask1417367048598452966.png';

exports.generateMIPs = async (searchParams) => {
    const nchannels = searchParams.channel;
    const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;

    const searchMaskId = getSearchMaskId(fullSearchInputImage);
    const mipsFolder = getSearchSubFolder(fullSearchInputImage, 'generatedMIPS');

    for(let channelNumber = 0; channelNumber < nchannels; channelNumber++) {
        const mipName = `${mipsFolder}/${searchMaskId}_U_20x_HR_0${channelNumber+1}.png`;
        await s3.copyObject({
            CopySource: `${TEST_IMAGE_BUCKET}/${TEST_IMAGE}`,
            Bucket: searchBucket,
            Key: mipName
        }).promise();
    }
    await updateSearchMetadata({
        id: searchParams.id || searchParams.searchId,
        step: ALIGNMENT_JOB_COMPLETED
    });
    return searchParams;
}
