import path from 'path';
import zlib from 'zlib';

import {GenerateColorMIPMasks, ColorMIPSearch} from './mipsearch';
import {loadMIPRange} from "./load_mip";
import {DEBUG, getObjectWithRetry, putDbItemWithRetry} from './utils';

const defaultBatchResultsMinToLive = process.env.BATCH_RESULTS_MIN_TO_LIVE || 15; // default ttl for batch results 15min if not set in the config

export const batchSearch = async (event) => {
    const { tasksTableName, jobId, batchId, startIndex, endIndex, jobParameters } = event;

    // The next three log statements are parsed by the analyzer. DO NOT CHANGE.
    console.log('Input event:', JSON.stringify(event));
    console.log(`Job Id: ${jobId}`);
    console.log(`Batch Id: ${batchId}`);
    logWithMemoryUsage(''); // log initial memory stats

    const batchParams = {
        jobId,
        batchId,
        tasksTableName,
        libraryBucket: jobParameters.libraryBucket,
        libraryThumbnailsBucket: jobParameters.libraryThumbnailsBucket,
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
    validateBatchParams(batchParams);
    const nresults = await executeColorDepthsSearches(batchParams, startIndex, endIndex);
    logWithMemoryUsage(`Completed Batch Id: ${batchId}`); // log final memory stats
    return nresults;
};

const validateBatchParams = (batchParams) => {
    if (!batchParams.libraries) {
        throw new Error('No target images to search');
    }
    if (!batchParams.maskKeys) {
        throw new Error('No masks to search');
    }
    if (!batchParams.maskThresholds) {
        throw new Error('No mask thresholds specified');
    }
    if (batchParams.maskThresholds.length !== batchParams.maskKeys.length) {
        throw new Error('Number of mask thresholds does not match number of masks');
    }
};

const executeColorDepthsSearches = async (batchParams, startIndex, endIndex) => {
    if (DEBUG) {
        logWithMemoryUsage(`Compare ${batchParams.maskKeys.length} masks with mips between [${startIndex},${endIndex}] from ${batchParams.libraries.length} libraries`);
    }
    const cdsResults = await findAllColorDepthMatches({
        maskKeys: batchParams.maskKeys,
        maskThresholds: batchParams.maskThresholds,
        libraries: batchParams.libraries,
        awsMasksBucket: batchParams.searchBucket,
        awsLibrariesBucket: batchParams.libraryBucket,
        awsLibrariesThumbnailsBucket: batchParams.libraryThumbnailsBucket,
        dataThreshold: batchParams.dataThreshold,
        pixColorFluctuation: batchParams.pixColorFluctuation,
        xyShift: batchParams.xyShift,
        mirrorMask: batchParams.mirrorMask,
        minMatchingPixRatio: batchParams.minMatchingPixRatio
    }, startIndex, endIndex);
    logWithMemoryUsage(`Batch Id: ${batchParams.batchId} - found ${cdsResults.length} matches.`);
    await writeCDSResults(cdsResults, batchParams.tasksTableName, batchParams.jobId, batchParams.batchId);
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
    return searchableTargets.flatMap(l => l.searchableKeys).slice(startIndex, endIndex);
};

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

const getKeys = async (libraryBucket, libraryKey) => {
    const randomPrefix = getRandomInt(0, 99);
    const keyName = `${libraryKey}/KEYS/${randomPrefix}/keys_denormalized.json`;
    logWithMemoryUsage(`Get keys from: ${keyName}`);
    return await getObjectWithRetry(libraryBucket, keyName);
};

const groupBy = (...keys) => xs =>
    xs.reduce(updateGB(...keys), []);

const updateGB = (...keys) => (acc, e) => {
    const foundI = acc.findIndex(d => keys.every( key => d[key] === e[key]));
    const divided = divProps(...keys)(e);
    if (foundI === -1) {
        return [...acc, {...divided.labels, results: [divided.results]}];
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
        mirrored: params.mirrored,
        gradientAreaGap: params.gradientAreaGap,
        normalizedScore: params.matchingPixels
    };
};

export const findAllColorDepthMatches = async (params, startIndex, endIndex) => {
    const libraryKeys = await getSearchKeys(params.awsLibrariesBucket, params.libraries, startIndex, endIndex);
    if (DEBUG) {
        logWithMemoryUsage(`Loaded ${libraryKeys.length} search keys: [${startIndex}, ${endIndex}]`);
    }
    const cdsPromise = await params.maskKeys
        .map(async (maskKey, maskIndex) => await runMaskSearches({
            maskKey: maskKey,
            maskThreshold: params.maskThresholds[maskIndex],
            libraryKeys: libraryKeys,
            awsMasksBucket: params.awsMasksBucket,
            awsLibrariesBucket: params.awsLibrariesBucket,
            awsLibrariesThumbnailsBucket: params.awsLibrariesThumbnailsBucket,
            dataThreshold: params.dataThreshold,
            pixColorFluctuation: params.pixColorFluctuation,
            xyShift: params.xyShift,
            mirrorMask: params.mirrorMask,
            minMatchingPixRatio: params.minMatchingPixRatio
        }));
    const cdsResults = await Promise.all(cdsPromise);
    return cdsResults.flat();
};

const runMaskSearches = async (params) => {
    const maskMetadata = getMaskMIPMetdata(params.awsMasksBucket, params.maskKey);

    const zTolerance = params.pixColorFluctuation == null ? 0.0 : params.pixColorFluctuation / 100.0;
    const maskThreshold = params.maskThreshold != null ? params.maskThreshold : 0;

    const maskImage = await loadMIPRange(params.awsMasksBucket, params.maskKey, 0, 0);

    const cdMask = GenerateColorMIPMasks({
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
    if (!cdMask.maskPositions) {
        // mask is empty
        console.log(`Empty mask image: ${params.maskKey}`);
        return [];
    }
    const pixMatchRatioThreshold = params.minMatchingPixRatio != null ? params.minMatchingPixRatio / 100.0 : 0.0;
    let results = [];
    for (let i = 0; i < params.libraryKeys.length; i++) {
        const tarImage = await loadMIPRange(params.awsLibrariesBucket, params.libraryKeys[i], cdMask.maskpos_st, cdMask.maskpos_ed);
        if (tarImage.data != null) {
            const sr = ColorMIPSearch(tarImage.data, params.dataThreshold, zTolerance, cdMask);
            if (DEBUG) {
                console.log(`Comparison result with ${params.libraryKeys[i]}`, sr, `mask size: ${cdMask.maskPositions.length}`);
                logWithMemoryUsage(`Compared ${params.maskKey} with ${params.libraryKeys[i]}`);
            }
            if (sr.matchingPixNumToMaskRatio > pixMatchRatioThreshold) {
                const r = {
                    maskMIP: maskMetadata,
                    libraryMIP: getLibraryMIPMetadata(params.libraryKeys[i]),
                    matchingPixels: sr.matchingPixNum,
                    matchingRatio: sr.matchingPixNumToMaskRatio,
                    mirrored: sr.bestScoreMirrored,
                    isMatch: true,
                    isError: false,
                    gradientAreaGap: -1
                };
                if (DEBUG) {
                    console.log(`Match found between ${params.maskKey} and ${params.libraryKeys[i]}`, r);
                }
                results.push(r);
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

const getLibraryMIPMetadata = (mipKey) => {
    const mipPath = path.parse(mipKey);
    const mipName = mipPath.name;
    const mipExt = mipPath.ext;
    // displayable mips are always png and the thumbnails jpg
    const mipImageKey = !mipExt
        ? getDisplayableMIPKey(mipKey) + '.png'
        // keep in mind that the ext returned by path contains the dot
        // because there are cases when displayable mip does not have the extension
        // we remove it first to guarantee is never there and then append it to guarantee it will always append it
        : getDisplayableMIPKey(mipKey).replace(new RegExp('\\' + mipExt +  '$'), '') + '.png';
    const mipThumbnailKey = mipImageKey.replace(new RegExp('\\.(png|tif)$'), '.jpg');
    const mipDirNames = mipKey.split("/");
    const nPathComponents = mipDirNames.length;
    let mip = {
        id: mipName,
        cdmPath: mipKey,
        imageName: mipKey,
        imageURL: `${mipImageKey}`, // use relative names
        thumbnailURL: `${mipThumbnailKey}`, // use relative names
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
    const groups = mipKey.match(reg).groups;
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
            mipMetadata["channel"] = matched[1];
        }
    }

    return mipMetadata;
};

const populateEMMetadataFromName = (mipName, mipMetadata) => {
    const mipNameComponents = mipName.split("-");
    mipMetadata["publishedName"] = mipNameComponents.length > 0 ? mipNameComponents[0] : mipName;
    mipMetadata["gender"] = "f"; // default to female for now
    return mipMetadata;
};


const writeCDSResults = async (cdsResults, tableName, jobId, batchId) => {
    const ttlDelta = defaultBatchResultsMinToLive * 60; // 20 min TTL
    const ttl = (Math.floor(+new Date() / 1000) + ttlDelta).toString();

    const matchedMetadata = cdsResults
        .map(perMaskMetadata)
        .sort(function(a, b) { return a.matchingPixels < b.matchingPixels ? 1 : -1; });

    const groupedResults = groupBy('maskId', 'maskLibraryName', 'maskPublishedName', 'maskImageURL')(matchedMetadata);
    const resultsSValue = JSON.stringify(groupedResults);

    const resultsAttr = resultsSValue.length < 4096
        ? { results: {S: resultsSValue} }
        : { resultsMimeType: {S: 'application/gzip'}, results: {B: zlib.gzipSync(resultsSValue)} };

    const item = {
        "jobId": {S: jobId},
        "batchId": {N: ""+batchId},
        "ttl": {N: ttl},
        ...resultsAttr
    };

    return await putDbItemWithRetry(tableName, item);
};

const logWithMemoryUsage = (msg) => {
    var mem = process.memoryUsage();
    console.log(msg,
        `Memory usage:`,
        `resident: ${mem.rss / 1048576} MB`,
        `available: ${mem.heapTotal / 1048576} MB`,
        `used: ${mem.heapUsed / 1048576} MB`,
        `external: ${mem.external / 1048576} MB`,
        `arrayBuffers: ${mem.arrayBuffers / 1048576} MB`
    );
};
