import AWS from 'aws-sdk';
import path from 'path';

import {GenerateColorMIPMasks, ColorMIPSearch} from './mipsearch';
import {loadMIPRange} from "./load_mip";
import {getObjectWithRetry} from './utils';

export const batchSearch = async (event) => {

    const { tasksTableName, jobId, batchId, startIndex, endIndex, jobParameters } = event;

    // The next three log statements are parsed by the analyzer. DO NOT CHANGE.
    console.log('Input event:', JSON.stringify(event));
    console.log(`Job Id: ${jobId}`);
    console.log(`Batch Id: ${batchId}`);

    const batchParams = {
        libraryBucket: jobParameters.libraryBucket,
        libraries: jobParameters.libraries,
        searchBucket: jobParameters.searchBucket,
        maskKeys: jobParameters.maskKeys,
        dataThreshold: jobParameters.dataThreshold || 100,
        maskThresholds: jobParameters.maskThresholds,
        pixColorFluctuation: jobParameters.pixColorFluctuation || 2.0,
        xyShift: jobParameters.xyShift || 0,
        mirrorMask: jobParameters.mirrorMask || false,
        minMatchingPixRatio: jobParameters.minMatchingPixRatio || 2.0
    };

    if (!batchParams.libraries) {
        throw new Error('No target images to search');
    }
    if (!batchParams.maskKeys) {
        throw new Error('No masks to search');
    }
    if (!batchParams.maskThresholds) {
        throw new Error('No mask thresholds specified');
    }
    if (batchParams.maskThresholds.length != batchParams.maskKeys.length) {
        throw new Error('Number of mask thresholds does not match number of masks');
    }
    const searchKeys = await getSearchKeys(batchParams.libraryBucket, batchParams.libraries, startIndex, endIndex);
    console.log(`Loaded ${searchKeys.length} search keys`);

    console.log(`Comparing ${batchParams.maskKeys.length} masks with ${searchKeys.length} library mips`);
    let cdsResults = await findAllColorDepthMatches({
        maskKeys: batchParams.maskKeys,
        maskThresholds: batchParams.maskThresholds,
        libraryKeys: searchKeys,
        awsMasksBucket: batchParams.searchBucket,
        awsLibrariesBucket: batchParams.libraryBucket,
        awsLibrariesThumbnailsBucket: process.env.SEARCHED_THUMBNAILS_BUCKET || batchParams.libraryBucket,
        dataThreshold: batchParams.dataThreshold,
        pixColorFluctuation: batchParams.pixColorFluctuation,
        xyShift: batchParams.xyShift,
        mirrorMask: batchParams.mirrorMask,
        minMatchingPixRatio: batchParams.minMatchingPixRatio
    });
    console.log(`Found ${cdsResults.length} matches.`);

    const matchedMetadata = cdsResults.map(perMaskMetadata)
        .sort(function(a, b) { return a.matchingPixels < b.matchingPixels ? 1 : -1; });
    const ret = groupBy("maskId","maskLibraryName", "maskPublishedName", "maskImageURL")(matchedMetadata);

    await writeCDSResults(ret, tasksTableName, jobId, batchId);
    console.log("Wrote results to DynamoDB");

    return cdsResults.length;
};

const getSearchKeys = async (libraryBucket, libraries, startIndex, endIndex) => {
    const searchableTargetsPromise =  await libraries
        .map(async key => {
            return await {
                searchableKeys: await getKeys(libraryBucket, key)
            };
        });
    const searchableTargets = await Promise.all(searchableTargetsPromise);
    const allTargets = searchableTargets.flatMap(l => l.searchableKeys);
    return allTargets.slice(startIndex, endIndex);
};

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

const getKeys = async (libraryBucket, libraryKey) => {
    const randomPrefix = getRandomInt(0, 99);
    const keyName = `${libraryKey}/KEYS/${randomPrefix}/keys_denormalized.json`;
    console.log(`Get keys from: ${keyName}`);
    return await getObjectWithRetry(libraryBucket, keyName);
};

const groupBy = (...keys) => xs =>
    xs.reduce(updateGB(...keys), []);

const updateGB = (...keys) => (acc, e) => {
    const foundI = acc.findIndex( d => keys.every( key => d[key] === e[key]));
    const divided = divProps(...keys)(e);
  if (foundI === -1) {
    return [...acc, {...divided.labels, results:[divided.results]}];
  }
  acc[foundI].results = [...acc[foundI].results, divided.results];
  return acc;
};

const divProps =(...keys) => e =>
    Object.entries(e).reduce(
        ( acc, [k, v] ) =>
            keys.includes(k)? {...acc, labels:{...acc.labels, [k]:v}}
                : {...acc, results:{...acc.results, [k]:v}}
        , {labels:{}, results:{}}
    );

const perMaskMetadata = (params) => {
    return {
        maskId: params.maskMIP.id,
        maskLibraryName: params.maskMIP.libraryName || null,
        maskPublishedName: params.maskMIP.publishedName || null,
        maskImageArchivePath: params.maskMIP.imageArchivePath,
        maskImageName: params.maskMIP.imageName,
        maskImageType: params.maskMIP.imageType,
        maskImageURL: params.maskMIP.imageURL,

        imageURL: params.libraryMIP.imageURL,
        thumbnailURL: params.libraryMIP.thumbnailURL,

        id: params.libraryMIP.id,
        libraryName: params.libraryMIP.libraryName,
        publishedName: params.libraryMIP.publishedName,
        imageArchivePath: params.libraryMIP.imageArchivePath,
        imageName: params.libraryMIP.imageName,
        imageType: params.libraryMIP.imageType,

        slideCode: params.libraryMIP.slideCode,
        objective: params.libraryMIP.objective,
        gender: params.libraryMIP.gender,
        anatomicalArea: params.libraryMIP.anatomicalArea,
        alignmentSpace: params.libraryMIP.alignmentSpace,
        channel: params.libraryMIP.channel,
        mountingProtocol: params.libraryMIP.mountingProtocol,

        matchingPixels: params.matchingPixels,
        matchingRatio: params.matchingRatio,
        gradientAreaGap: params.gradientAreaGap,
        normalizedScore: params.matchingPixels
    };
};

export const findAllColorDepthMatches = async (params) => {
    const maskKeys = params.maskKeys;

    let results = [];
    let i;
    for (i = 0; i < maskKeys.length; i++) {
        results.push(await runMaskSearches({
            maskKey: params.maskKeys[i],
            maskThreshold: params.maskThresholds[i],
            libraryKeys: params.libraryKeys,
            awsMasksBucket: params.awsMasksBucket,
            awsLibrariesBucket: params.awsLibrariesBucket,
            awsLibrariesThumbnailsBucket: params.awsLibrariesThumbnailsBucket,
            dataThreshold: params.dataThreshold,
            pixColorFluctuation: params.pixColorFluctuation,
            xyShift: params.xyShift,
            mirrorMask: params.mirrorMask,
            minMatchingPixRatio: params.minMatchingPixRatio
        }));
    }
    return results.flat();
};

const runMaskSearches = async (params) => {
    const maskMetadata = getMaskMIPMetdata(params.awsMasksBucket, params.maskKey);

    const zTolerance = params.pixColorFluctuation == null ? 0.0 : params.pixColorFluctuation / 100.0;
    const maskThreshold = params.maskThreshold != null ? params.maskThreshold : 0;

    let maskImage = await loadMIPRange(params.awsMasksBucket, params.maskKey, 0, 0);

    const masks = GenerateColorMIPMasks({
        width: maskImage.width,
        height: maskImage.height,
        queryImage: maskImage.data,
        maskThreshold: maskThreshold,
        negQueryImage: null,
        negMaskThreshold: 0,
        xyShift: params.xyShift,
        mirrorMask: params.mirrorMask,
        mirrorNegMask: false
    });

    let results = [];
    let i;
    for (i = 0; i < params.libraryKeys.length; i++) {
        const libMetadata = getLibraryMIPMetadata(params.awsLibrariesBucket, params.awsLibrariesThumbnailsBucket, params.libraryKeys[i]);
        const tarImage = await loadMIPRange(params.awsLibrariesBucket, params.libraryKeys[i], masks.maskpos_st, masks.maskpos_ed);
        if (tarImage.data != null) {
            const sr = ColorMIPSearch(tarImage.data, params.dataThreshold, zTolerance, masks);
            const pixMatchRatioThreshold = params.minMatchingPixRatio != null ? params.minMatchingPixRatio / 100.0 : 0.0;
            if (sr.matchingPixNumToMaskRatio > pixMatchRatioThreshold) {
                results.push({
                    maskMIP: maskMetadata,
                    libraryMIP: libMetadata,
                    matchingPixels: sr.matchingPixNum,
                    matchingRatio: sr.matchingPixNumToMaskRatio,
                    isMatch: true,
                    isError: false,
                    gradientAreaGap: -1
                });
            }
        }
    }
    return results;
};

const getMaskMIPMetdata = (awsMasksBucket, mipKey) => {
    const mipPath = path.parse(mipKey);
    return {
        id: mipPath.name,
        cdmPath: mipKey,
        imageName: mipKey,
        imageURL: `https://s3.amazonaws.com/${awsMasksBucket}/${mipKey}`
    };
};

const getLibraryMIPMetadata = (awsLibrariesBucket, awsLibrariesThumbnailsBucket, mipKey) => {
    const mipPath = path.parse(mipKey);
    const mipName = mipPath.name;
    const mipExt = mipPath.ext;
    // displayable mips are always png and the thumbnails jpg
    let mipImageKey;
    if (!mipExt) {
        mipImageKey = getDisplayableMIPKey(mipKey) + '.png';
    } else {
        // keep in mind that the ext returned by path contains the dot
        // because there are cases when displayable mip does not have the extension
        // we remove it first to guarantee is never there and then append it to guarantee it will always append it
        mipImageKey = getDisplayableMIPKey(mipKey).replace(new RegExp('\\' + mipExt +  '$'), '') + '.png';
    }
    const mipThumbnailKey = mipImageKey.replace(new RegExp('\\.(png|tif)$'), '.jpg');
    const mipDirNames = mipKey.split("/");
    const nPathComponents = mipDirNames.length;
    let mip = {
        id: mipName,
        cdmPath: mipKey,
        imageName: mipKey,
        imageURL: `https://s3.amazonaws.com/${awsLibrariesBucket}/${mipImageKey}`,
        thumbnailURL: `https://s3.amazonaws.com/${awsLibrariesThumbnailsBucket}/${mipThumbnailKey}`,
        alignmentSpace: nPathComponents > 3 ? mipDirNames[0] : null,
        libraryName: nPathComponents > 3 ? mipDirNames[1] : mipDirNames[0],
    };
    if (nPathComponents > 3) {
        // the folder structure is <alignmentSpace>/<libraryName>/...images
        mip["alignmentSpace"] = mipDirNames[0];
        mip["libraryName"] = mipDirNames[1];
    } else if (nPathComponents > 2) {
        // the folder structure is <libraryName>/...images
        mip["libraryName"] = mipDirNames[0];
    }
    if (isEmLibrary(mip.libraryName)) {
        return populateEMMetadataFromName(mipName, mip);
    } else {
        return populateLMMetadataFromName(mipName, mip);
    }
};

const getDisplayableMIPKey = (mipKey) => {
    const reg = /.+(?<mipName>\/[^/]+(-CDM(_[^-]*)?)(?<cdmSuffix>-.*)?\..*$)/;
    let groups = mipKey.match(reg).groups;
    const removableGroupStart = groups && groups.cdmSuffix ? mipKey.indexOf(groups.cdmSuffix) : -1;
    const mipName = removableGroupStart > 0 ? mipKey.substring(0, removableGroupStart) : mipKey;
    return mipName
        .replace(/searchable_neurons\/\d+/, '')
        .replace('//', '/');
};

const isEmLibrary = (lname) => {
    return lname != null && lname.match(/flyem/i) && lname.match(/hemibrain/i);
};

const populateLMMetadataFromName = (mipName, mipMetadata) => {
    const mipNameComponents = mipName.split("-");
    const line = mipNameComponents.length > 0 ? mipNameComponents[0] : mipName;
    mipMetadata["publishedName"] = line;
    if (mipNameComponents.length >= 2) {
        mipMetadata["slideCode"] = mipNameComponents[1];
    }
    if (mipNameComponents.length >= 4) {
        mipMetadata["gender"] = mipNameComponents[3];
    }
    if (mipNameComponents.length >= 5) {
        mipMetadata["objective"] = mipNameComponents[4];
    }
    if (mipNameComponents.length >= 6) {
        mipMetadata["anatomicalArea"] = mipNameComponents[5];
    }
    if (mipNameComponents.length >= 7) {
        mipMetadata["alignmentSpace"] = mipNameComponents[6];
    }
    if (mipNameComponents.length >= 8) {
        const cdmWithChannel = mipNameComponents[7];
        const matched = cdmWithChannel.match(/CDM_(\d+)/i);
        if (matched.length >= 2) {
            const channel = matched[1];
            mipMetadata["channel"] = channel;
        }
    }

    return mipMetadata;
};

const populateEMMetadataFromName = (mipName, mipMetadata) => {
    const mipNameComponents = mipName.split("-");
    const bodyID = mipNameComponents.length > 0 ? mipNameComponents[0] : mipName;
    mipMetadata["publishedName"] = bodyID;
    mipMetadata["gender"] = "f"; // default to female for now
    return mipMetadata;
};

const writeCDSResults = async (results, tableName, jobId, batchId) => {

    const TTL_DELTA = 60 * 60; // 1 hour TTL
    const ttl = (Math.floor(+new Date() / 1000) + TTL_DELTA).toString();

    const params = {
        TableName: tableName,
        Item: {
            "jobId": {S: jobId},
            "batchId": {N: ""+batchId},
            "ttl": {N: ttl},
            "results": {S: JSON.stringify(results)}
        }
    };

    const ddb = new AWS.DynamoDB({apiVersion: '2012-08-10'});
    return await ddb.putItem(params).promise();
};
