'use strict';

const AWS = require('aws-sdk');
const AWSXRay = require('aws-xray-sdk-core');
const tiff = require('geotiff');
const path = require('path');

const {GenerateColorMIPMasks, ColorMIPSearch} = require('./mipsearch');
const {getSearchMetadataKey, getIntermediateSearchResultsKey} = require('./searchutils');
const {getObjectDataArray, getObject, putObject, invokeAsync, partition, verifyKey, DEBUG} = require('./utils');
const {getSearchMetadata, updateSearchMetadata, SEARCH_IN_PROGRESS} = require('./awsappsyncutils');

exports.batchSearch = async (event) => {
    const batchParams = {
        searchPrefix: event.searchPrefix,
        searchKeys: event.searchKeys,
        maskPrefix: event.maskPrefix,
        maskKeys: event.maskKeys,
        dataThreshold: event.dataThreshold || 100,
        maskThresholds: event.maskThresholds,
        pixColorFluctuation: event.pixColorFluctuation || 2.0,
        xyShift: event.xyShift || 0,
        mirrorMask: event.mirrorMask || false,
        outputBucket: event.outputBucket,
        outputKey: event.outputKey,
        minMatchingPixRatio: event.minMatchingPixRatio || 2.0
    }

    console.log(batchParams);
    const eventBody = JSON.parse(batchParams.body);
    console.log("Parsed body", eventBody);
    const segment = AWSXRay.getSegment();
    let subsegment = segment.addNewSubsegment('Read parameters');
    if (batchParams.searchPrefix == null) {
        console.log('No images to search');
        return 0;
    }
    if (batchParams.maskKeys == null) {
        console.log('No masks to search');
        return 0;
    }
    if (batchParams.maskThresholds == null) {
        console.log('No mask thresholds specified')
        return 0;
    }
    if (batchParams.maskThresholds.length != batchParams.maskKeys.length) {
        console.log('Number of mask thresholds does not match number of masks');
        return 0;
    }
    subsegment.close();


    subsegment = segment.addNewSubsegment('Read search');

    console.log(`Comparing ${batchParams.maskKeys.length} masks with ${batchParams.searchKeys.length} library mips`);
    let cdsResults = findAllColorDepthMatches({
        maskKeys: batchParams.maskKeys,
        maskThresholds: batchParams.maskThresholds,
        libraryKeys: batchParams.searchKeys,
        awsMasksBucket: batchParams.maskPrefix,
        awsLibrariesBucket: batchParams.searchPrefix,
        awsLibrariesThumbnailsBucket: process.env.SEARCHED_THUMBNAILS_BUCKET || batchParams.searchPrefix,
        dataThreshold: batchParams.dataThreshold,
        pixColorFluctuation: batchParams.pixColorFluctuation,
        xyShift: batchParams.xyShift,
        mirrorMask: batchParams.mirrorMask,
        minMatchingPixRatio: batchParams.minMatchingPixRatio
    });
    console.log(`Found ${cdsResults.length} matches.`);

    subsegment.close();

    if (batchParams.outputURI != null) {

        const matchedMetadata = cdsResults.map(perMaskMetadata)
            .sort(function(a, b) {return a.matchingPixels < b.matchingPixels ? 1 : -1;});
        const ret = groupBy("sourceId","sourcePublishedName", "sourceLibraryName", "sourceImageURL")(matchedMetadata);

        subsegment = segment.addNewSubsegment('Sort and save results');
        await putObject(
            batchParams.outputBucket,
            batchParams.outputKey,
            ret);
        subsegment.close();
    }

    return cdsResults.length;

}

const groupBy = (...keys) => xs =>
    xs.reduce(updateGB(...keys), [])

const updateGB = (...keys) => (acc, e) => {
    const foundI = acc.findIndex( d => keys.every( key => d[key] === e[key]))
    const divided = divProps(...keys)(e)
    return foundI === -1  ? [...acc, {...divided.labels, data:[divided.data]}]
        : (acc[foundI].data = [...acc[foundI].data, divided.data], acc)
}

const divProps =(...keys) => e =>
    Object.entries(e).reduce(
        ( acc, [k, v] ) =>
            keys.includes(k)? {...acc, labels:{...acc.labels, [k]:v}}
                : {...acc, data:{...acc.data, [k]:v}}
        , {labels:{}, data:{}}
    )

const perMaskMetadata = (params) => {
    return {
        sourceId: params.maskMIP.id,
        sourceLibraryName: params.maskMIP.libraryName,
        sourcePublishedName: params.maskMIP.publishedName,
        sourceImageArchivePath: params.maskMIP.imageArchivePath,
        sourceImageName: params.maskMIP.imageName,
        sourceImageType: params.maskMIP.imageType,
        sourceImageURL: params.maskMIP.imageURL,

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
}

const findAllColorDepthMatches = (params) => {
    const maskKeys = params.maskKeys;
    const maskThresholds = params.maskThresholds;
    const libraryKeys = params.libraryKeys;
    const awsMasksBucket = params.awsMasksBucket;
    const awsLibrariesBucket = params.awsLibrariesBucket;
    const awsLibrariesThumbnailsBucket = params.awsLibrariesThumbnailsBucket;

    let results = [];
    let i;
    for (i = 0; i < maskKeys.length; i++) {
        results.push(runMaskSearches({
            maskKey: params.maskKey[i],
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
}

const runMaskSearches = async (params) => {
    const maskMetadata = getMaskMIPMetdata(params.maskKey);
    const maskfile = getObjectDataArray(params.awsMasksBucket, params.maskKey);
    const maskImage = await tiff.fromArrayBuffer(await maskfile).getImage();
    if (maskImage == null) {
        return null;
    }
    const maskImageArray = maskImage.readRasters({ interleave: true });
    const width = maskImage.getWidth();
    const height = maskImage.getHeight();
    const zTolerance = params.pixColorFluctuation == null ? 0.0 : params.pixColorFluctuation / 100.0;
    const maskThreshold = params.maskThreshold != null ? params.maskThreshold : 0;

    let results = [];
    let i;
    for (i = 0; i < params.libraryKeys.length; i++)
    {
        const libMetadata = getLibraryMIPMetadata(params.awsLibrariesBucket, params.awsLibrariesThumbnailsBucket, params.libraryKeys[i]);
        const masks = GenerateColorMIPMasks({
            width: width,
            height: height,
            queryImage: maskImageArray,
            maskThreshold: maskThreshold,
            negQueryImage: null,
            negMaskThreshold: 0,
            xyShift: params.xyShift,
            mirrorMask: params.mirrorMask,
            mirrorNegMask: false
        });
        const tarimage = loadMIPRange(params.awsLibrariesBucket, libMetadata, masks.maskpos_st, masks.maskpos_ed);
        if (tarimage != null) {
            const sr = ColorMIPSearch(tarimage, params.dataThreshold, zTolerance, masks);
            const pixMatchRatioThreshold = params.minMatchingPixRatio != null ? params.minMatchingPixRatio / 100.0 : 0.;
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
}

const loadMIPRange = (bucketName, metadata, start, end) => {

}

const getMaskMIPMetdata = (awsMasksBucket, mipKey) => {
    const mipPath = path.parse(mipKey);
    return {
        id: mipPath.name,
        cdmPath: mipKey,
        imageName: mipKey,
        imageURL: `https://s3.amazonaws.com/${awsMasksBucket}/${mipKey}`
    };
}

const getLibraryMIPMetadata = (awsLibrariesBucket, awsLibrariesThumbnailsBucket, mipKey) => {
    const mipPath = path.parse(mipKey);
    const mipName = mipPath.name;
    const mipExt = mipPath.ext;

    // displayable mips are always png and the thumbnails jpg
    let mipImageKey;
    if (mipExt == null) {
        mipImageKey = getDisplayableMIPKey(mipKey);
    } else {
        const re = new RegExp("\\." + mipExt + "$");
        mipImageKey = getDisplayableMIPKey(mipKey).replace(re, ".png");
    }
    const mipThumbnailKey = mipImageKey.replace("\\.png$", ".jpg");
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
}

const getDisplayableMIPKey = (mipKey) => {
    const reg = /.+(?<mipName>\/[^\/]+(-CDM(_[^-]*)?)(?<cdmSuffix>-.*)?\..*$)/;
    let groups = mipKey.match(reg).groups;
    if (groups) {
        let displayableKeyName = "";
        let namePos = 0;
        if (groups.cdmSuffix) {
            const removableGroupStart = mipKey.indexOf(groups.cdmSuffix);
            if (removableGroupStart > 0) {
                displayableKeyName +=
                    mipKey.substring(namePos, removableGroupStart)
                        .replace("searchable_neurons", "")
                        .replace("//", "/");

                namePos = removableGroupStart + groups.cdmSuffix.length;
            }
        }
        displayableKeyName +=
            mipKey.substring(namePos)
                .replace("searchable_neurons", "")
                .replace("//", "/");
        return displayableKeyName;
    } else {
        return mipKey
            .replace("searchable_neurons", "")
            .replace("//", "/");
    }
}

const isEmLibrary = (lname) => {
    return lname != null && lname.match(/flyem/i) && lname.match(/hemibrain/i);
}

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
}

const populateEMMetadataFromName = (mipName, mipMetadata) => {
    const mipNameComponents = mipName.split("-");
    const bodyID = mipNameComponents.length > 0 ? mipNameComponents[0] : mipName;
    mipMetadata["publishedName"] = bodyID;
    mipMetadata["gender"] = "f"; // default to female for now
    return mipMetadata;
}