import AWS from 'aws-sdk';
import {getSearchMaskId, getSearchSubFolder} from './searchutils';
import {updateSearchMetadata, ALIGNMENT_JOB_COMPLETED} from './awsappsyncutils';

const s3 = new AWS.S3();

const searchBucket = process.env.SEARCH_BUCKET;
const TEST_IMAGE_BUCKET = process.env.SEARCH_BUCKET;
const TEST_IMAGE = 'colorDepthTestData/test1/mask1417367048598452966.png';
const MIPS_FOLDER = 'generatedMIPS';

exports.generateMIPs = async (searchParams) => {
    console.log('Generate fake MIPs for', searchParams);
    const nchannels = searchParams.channel;
    const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;

    const searchMaskId = getSearchMaskId(fullSearchInputImage);
    const mipsFolder = getSearchSubFolder(fullSearchInputImage, MIPS_FOLDER);
    let mips = [];
    let displayableMask = null;

    for(let channelNumber = 0; channelNumber < nchannels; channelNumber++) {
        const mipName = `${searchMaskId}_U_20x_HR_0${channelNumber+1}.png`;
        await s3.copyObject({
            CopySource: `${TEST_IMAGE_BUCKET}/${TEST_IMAGE}`,
            Bucket: searchBucket,
            Key: `${mipsFolder}/${mipName}`
        }).promise();
        const mip = `${MIPS_FOLDER}/${mipName}`;
        if (channelNumber === 0) {
            displayableMask = mip;
        }
        mips.push(mip);
    }
    console.log('Set diplayableMask and mips', searchParams, displayableMask, mips);
    return await updateSearchMetadata({
        id: searchParams.id || searchParams.searchId,
        step: ALIGNMENT_JOB_COMPLETED,
        displayableMask: displayableMask,
        computedMIPs: mips
    });
};
