import path from 'path';
import zlib from 'zlib';

import {GenerateColorMIPMasks, ColorMIPSearch} from './mipsearch';
import {loadMIPRange} from "./load_mip";
import { DEBUG, getObjectWithRetry } from './utils';

const defaultBatchResultsMinToLive = process.env.BATCH_RESULTS_MIN_TO_LIVE || 15; // default ttl for batch results 15min if not set in the config

export const batchSearch = async (event) => {
    const { tasksTableName, jobId, batchId, startIndex, endIndex, jobParameters } = event;

    // The next three log statements are parsed by the analyzer. DO NOT CHANGE.
    console.log('Input event:', JSON.stringify(event));
    console.log(`Job Id: ${jobId}`);
    console.log(`Batch Id: ${batchId}`);
    logWithMemoryUsage(''); // log initial memory stats

    // jobParameters.libraries is an array of objects containing:
    // {
    //     store: <string>
    //     libraryBucket: <string>,
    //     libraryThumbnailsBucket: <string>,
    //     alignmentSpace: <string>,
    //     libraryName: <string>,
    //     publishedNamePrefix: <string>,
    //     searchedNeuronsFolder: <string>,
    //     lsize: <number>
    // }
    const batchParams = {
        jobId,
        batchId,
        tasksTableName,
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
    const cdsResults = await executeColorDepthsSearches(batchParams, startIndex, endIndex);
    logWithMemoryUsage(`Completed Batch Id: ${batchId}`); // log final memory stats
    return cdsResults;
};

const validateBatchParams = (batchParams) => {
    if (!batchParams.libraries) {
        throw new Error('No target images to search');
    }
    // validate each library
    batchParams.libraries.forEach(l => {
        if (!l.libraryBucket) {
            throw new Error(`No bucket specified for library ${l.libraryName}`);
        } else if (!l.searchedNeuronsFolder) {
            throw new Error(`No search prefix specified for library ${l.libraryName}`);
        }
    });
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
        masksBucket: batchParams.searchBucket,
        maskKeys: batchParams.maskKeys,
        maskThresholds: batchParams.maskThresholds,
        libraries: batchParams.libraries,
        dataThreshold: batchParams.dataThreshold,
        pixColorFluctuation: batchParams.pixColorFluctuation,
        xyShift: batchParams.xyShift,
        mirrorMask: batchParams.mirrorMask,
        minMatchingPixRatio: batchParams.minMatchingPixRatio
    }, startIndex, endIndex);
    logWithMemoryUsage(`Batch Id: ${batchParams.batchId} - found ${cdsResults.length} matches.`);
    const finalCDSResults = createFinalCDSResults(cdsResults, batchParams.jobId, batchParams.batchId);
    // await putDbItemWithRetry(batchParams.tasksTableName, finalCDSResults);
    return finalCDSResults;
};

const findAllColorDepthMatches = async (params, startIndex, endIndex) => {
    const searchedMIPs = await getSearchedMIPs(params.libraries, startIndex, endIndex);
    if (DEBUG) {
        logWithMemoryUsage(`Loaded ${searchedMIPs.length} search keys: [${startIndex}, ${endIndex}]`);
    }
    const cdsPromise = await params.maskKeys
        .map(async (maskKey, maskIndex) => await runMaskSearches({
            masksBucket: params.masksBucket,
            maskKey: maskKey,
            maskThreshold: params.maskThresholds[maskIndex],
            targetMIPs: searchedMIPs,
            dataThreshold: params.dataThreshold,
            pixColorFluctuation: params.pixColorFluctuation,
            xyShift: params.xyShift,
            mirrorMask: params.mirrorMask,
            minMatchingPixRatio: params.minMatchingPixRatio
        }));
    const cdsResults = await Promise.all(cdsPromise);
    return cdsResults.flat();
};


const getSearchedMIPs = async (libraries, startIndex, endIndex) => {
    const initialValue = {
        currentIndex: 0,
        selectedLibraries: [],
    };
    const selectedLibraries = libraries.reduce(
        (previousValue, l) => {
            if (previousValue.currentIndex >= endIndex) {
                // this is past the selected range already so simply return
                // everything is to the right of the selected range (...)...[...]
                // selected libraries remain unchanged
                return {
                    currentIndex: previousValue.currentIndex + l.lsize,
                    selectedLibraries: previousValue.selectedLibraries,
                };
            }
            if (previousValue.currentIndex < startIndex) {
                // nothing has been selected yet
                if (previousValue.currentIndex + l.lsize < startIndex) {
                    // the selected range has not been reached yet
                    // everything is to the left of the selected range [...]...(...)
                    // selectedLibraries remain unchanged
                } else {
                    if (previousValue.currentIndex + l.lsize > endIndex) {
                        // the selected range is entirely inside this library [ ... (...) ... ]
                        previousValue.selectedLibraries.push({
                            library: l,
                            startRange: startIndex - previousValue.currentIndex,
                            endRange: endIndex - previousValue.currentIndex,
                        });
                    } else {
                        // add the portion of the MIPs that falls inside the selected range [ .. (... ] ...) ...]
                        previousValue.selectedLibraries.push({
                            library: l,
                            startRange: startIndex - previousValue.currentIndex,
                            endRange: l.lsize,
                        });
                    }
                }
                return {
                    currentIndex: previousValue.currentIndex + l.lsize,
                    selectedLibraries: previousValue.selectedLibraries,
                };
            } else {
                // this is still within the selected range
                if (previousValue.currentIndex + l.lsize < endIndex) {
                    // all MIPs from this library should be selected (... [...] ...)
                    previousValue.selectedLibraries.push({
                        library: l,
                        startRange: 0,
                        endRange: l.lsize,
                    });
                } else {
                    // the selected range ends with this library (... [ ...) ... ]
                    previousValue.selectedLibraries.push({
                        library: l,
                        startRange: 0,
                        endRange: endIndex - previousValue.currentIndex,
                    });
                }
                return {
                    currentIndex: previousValue.currentIndex + l.lsize,
                    selectedLibraries: previousValue.selectedLibraries,
                };
            }
        },
        initialValue
    ).selectedLibraries;

    const searchableTargetsPromise =  await selectedLibraries
        .map(async librarySelection => {
            const imageStore = librarySelection.library.store;
            const anatomicalArea = librarySelection.library.anatomicalArea;
            const targetType = librarySelection.library.targetType;
            const libraryBucket = librarySelection.library.libraryBucket;
            const libraryPrefix = librarySelection.library.searchedNeuronsFolder;
            const selectedMIPs = await getMIPs(
                libraryBucket,
                libraryPrefix,
                librarySelection.startRange,
                librarySelection.endRange
            );
            // add thumbnail bucket to the result
            return selectedMIPs.map(m => ({
                ...m,
                store: imageStore,
                anatomicalArea,
                targetType,
                alignmentSpace: librarySelection.library.alignmentSpace,
                libraryName: librarySelection.library.libraryName,
                publishedNamePrefix: librarySelection.library.publishedNamePrefix,
                thumbnailBucketName: librarySelection.library.libraryThumbnailsBucket,
            }));
        });

    const searchableTagets = await Promise.all(searchableTargetsPromise);
    // no need to slice the final result because we only selected the needed MIPs from library
    return searchableTagets.flat();
};

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

const getMIPs = async (libraryBucket, libraryKey, start, end) => {
    // all library MIPs are listed in a keys_denormalied.json
    // which is replicated in 100 locations
    // to get the MIP locations selects one location randomly
    // and reads the MIPs from the keys_denormalized.json located in the selected location
    const randomPrefix = getRandomInt(0, 99);
    const keyName = `${libraryKey}/KEYS/${randomPrefix}/keys_denormalized.json`;
    logWithMemoryUsage(`Get keys from: ${keyName}`);
    const allLibraryMips = await getObjectWithRetry(libraryBucket, keyName);
    return allLibraryMips.slice(start, end).map(mip => ({
        bucketName: libraryBucket,
        mipKey: mip,
    }));
};

const perMaskMetadata = (params) => {
    return {
        maskId: params.maskMIP.id,
        maskLibraryName: params.maskMIP.libraryName || null,
        maskPublishedName: params.maskMIP.publishedName || null,
        maskImageName: params.maskMIP.imageName,
        maskImageURL: params.maskMIP.imageURL,

        imageURL: params.libraryMIP.imageURL,
        thumbnailURL: params.libraryMIP.thumbnailURL,

        id: params.libraryMIP.id,
        libraryStore: params.libraryMIP.store,
        targetType: params.libraryMIP.targetType,
        libraryName: params.libraryMIP.libraryName,
        publishedName: params.libraryMIP.publishedName,
        publishedNamePrefix: params.libraryMIP.publishedNamePrefix,
        imageName: params.libraryMIP.imageName,

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

const runMaskSearches = async (params) => {
    const maskMetadata = getMaskMIPMetdata(params.masksBucket, params.maskKey);

    const zTolerance = params.pixColorFluctuation == null ? 0.0 : params.pixColorFluctuation / 100.0;
    const maskThreshold = params.maskThreshold != null ? params.maskThreshold : 0;

    const maskImage = await loadMIPRange(params.masksBucket, params.maskKey, 0, 0);

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
    for (let i = 0; i < params.targetMIPs.length; i++) {
        const tarImage = await loadMIPRange(params.targetMIPs[i].bucketName, params.targetMIPs[i].mipKey, cdMask.maskpos_st, cdMask.maskpos_ed);
        if (tarImage.data != null) {
            const sr = ColorMIPSearch(tarImage.data, params.dataThreshold, zTolerance, cdMask);
            if (DEBUG) {
                console.log(`Comparison result with ${params.targetMIPs[i]}`, sr, `mask size: ${cdMask.maskPositions.length}`);
                logWithMemoryUsage(`Compared ${params.maskKey} with ${params.targetMIPs[i]}`);
            }
            if (sr.matchingPixNumToMaskRatio > pixMatchRatioThreshold) {
                const r = {
                    maskMIP: maskMetadata,
                    libraryMIP: getLibraryMIPMetadata(params.targetMIPs[i]),
                    matchingPixels: sr.matchingPixNum,
                    matchingRatio: sr.matchingPixNumToMaskRatio,
                    mirrored: sr.bestScoreMirrored,
                    isMatch: true,
                    isError: false,
                    gradientAreaGap: -1
                };
                if (DEBUG) {
                    console.log(`Match found between ${params.maskKey} and ${params.targetMIPs[i]}`, r);
                }
                results.push(r);
            }
        }
    }
    return results;
};

const getMaskMIPMetdata = (maskBucket, mipKey) => {
    const mipPath = path.parse(mipKey);
    return {
        id: mipPath.name,
        cdmPath: mipKey,
        imageName: mipKey,
        imageURL: `https://s3.amazonaws.com/${maskBucket}/${mipKey}`
    };
};

const getLibraryMIPMetadata = (libraryMip) => {
    const mipPath = path.parse(libraryMip.mipKey);
    const mipName = mipPath.name;
    const mipExt = mipPath.ext;
    // displayable mips are always png and the thumbnails jpg
    const mipImageKey = !mipExt
        ? getDisplayableMIPKey(libraryMip.mipKey) + '.png'
        // keep in mind that the ext returned by path contains the dot
        // because there are cases when displayable mip does not have the extension
        // we remove it first to guarantee is never there and then append it to guarantee it will always append it
        : getDisplayableMIPKey(libraryMip.mipKey).replace(new RegExp('\\' + mipExt +  '$'), '') + '.png';
    const mipThumbnailKey = mipImageKey.replace(new RegExp('\\.(png|tif)$'), '.jpg');
    let mip = {
        id: mipName,
        store: libraryMip.store,
        anatomicalArea: libraryMip.anatomicalArea,
        targetType: libraryMip.targetType,
        cdmPath: libraryMip.mipKey,
        imageName: libraryMip.mipKey,
        imageURL: `${mipImageKey}`, // use relative names
        thumbnailURL: `${mipThumbnailKey}`, // use relative names
        alignmentSpace: libraryMip.alignmentSpace,
        libraryName: libraryMip.libraryName,
        publishedNamePrefix: libraryMip.publishedNamePrefix,
    };
    if (libraryMip.targetType === 'EMImage') {
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

const createFinalCDSResults = (cdsResults, jobId, batchId) => {
    const matchedMetadata = cdsResults
        .map(perMaskMetadata)
        .sort(function(a, b) { return a.matchingPixels < b.matchingPixels ? 1 : -1; });

    const groupedResults = groupBy('maskId', 'maskLibraryName', 'maskPublishedName', 'maskImageURL')(matchedMetadata);

    const resultsSValue = JSON.stringify(groupedResults);

    const resultsAttr = resultsSValue.length < 8192
        ? {
            resultsMimeType: 'application/json',
            results: resultsSValue
        }
        : {
            resultsMimeType: 'application/gzip',
            results: zlib.gzipSync(resultsSValue).toString('base64')
        };

    const ttlDelta = defaultBatchResultsMinToLive * 60; // 20 min TTL
    const ttl = (Math.floor(+new Date() / 1000) + ttlDelta);

    return {
        jobId: jobId,
        batchId: batchId.toString(),
        nresults: cdsResults.length.toString(),
        ttl: ttl.toString(),
        ...resultsAttr,
    };
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
