import {
    DEBUG,
    getObjectWithRetry,
    getS3ContentWithRetry,
    verifyKey
} from './utils';
import {
    SEARCH_IN_PROGRESS,
    getSearchMetadata,
    updateSearchMetadata
} from './awsappsyncutils';

import { Map, Set } from 'immutable';

export const getSearchInputParams = async (event) => {
    let searchMetadata;
    // Both searchInputFolder and searchInputName must be provided because
    // the full input path is `${searchInputFolder}/${searchInputName}`
    if (!event.searchInputName || !event.searchInputFolder) {
        // If searchInputName or searchInputFolder is not given the searchId must be provided
        // so that the searchInput path can be retrieved from the database.
        const searchId = event.searchId;
        if (!searchId) {
            throw new Error('Missing required parameter: "searchId"');
        }
        searchMetadata = await getSearchMetadata(searchId);
    } else {
        searchMetadata = event;
    }
    if (!!searchMetadata && !!searchMetadata.searchMask) {
        // if a searchMask is set use that for search otherwise use the upload
        console.log(
            `Use ${searchMetadata.searchMask} for searching instead of ${searchMetadata.searchInputName}`
        );
        searchMetadata.searchInputName = searchMetadata.searchMask;
    }
    return searchMetadata;
};

export const checkSearchMask = async (searchId, bucket, maskKey) => {
    const checkMaskFlag = await verifyKey(bucket, maskKey);
    if (checkMaskFlag === false) {
        const errMsg = `Mask s3://${bucket}/${maskKey} not found`;
        // set the error
        await updateSearchMetadata({
            id: searchId,
            step: SEARCH_IN_PROGRESS,
            errorMessage: errMsg
        });
        throw new Error(errMsg);
    }
};

/**
 * Create search libraries based on anatomicalRegion and searchType from cdsConfig.
 *
 * @param searchData
 */
export const getSearchedLibraries = async (searchData, dataBucket) => {
    const anatomicalRegion = searchData.anatomicalRegion || 'brain';
    console.log(`Getting search libraries for ${anatomicalRegion}:${searchData.searchType}`);
    const dataConfig = await getDataConfig(dataBucket);
    // find all enabled datasets for the specified anatomical area
    const searchCfgs = Object.keys(dataConfig.dataSets)
                        .map(ds => dataConfig.dataSets[ds])
                        .filter(cfg => {
                            return cfg.anatomicalArea.toLowerCase() === anatomicalRegion.toLowerCase() && !cfg.disabled
                        })
    if (!searchCfgs) {
        console.error(`No CDS configuration found for ${anatomicalRegion}:${searchData.searchType} in`, dataConfig);
        return {
            ...searchData,
            totalSearches: 0,
            searchedLibraries,
        };
    }
    const searchType = searchData.searchType;
    let libraryNamesGetter;
    if (searchType === 'em2lm' || searchType === 'lmTarget') {
        // from all matching configurations collect 'lmLibraries' together with alignmentSpace and bucket
        libraryNamesGetter = cfg => cfg.lmLibraries;
    } else if (searchType === 'lm2em' || searchType === 'emTarget') {
        // from all matching configurations collect 'emLibraries' together with alignmentSpace and bucket
        libraryNamesGetter = cfg => cfg.emLibraries;
    } else {
        console.error(`Unsupported searchType: ${searchType}`, searchData);
        libraryNamesGetter = cfg => [];
    }
    const searchedLibraries = await getAllSearchedLibrariesWithSizes(searchCfgs, libraryNamesGetter);
    const totalSearches = searchedLibraries
        .map(l => l.lsize)
        .reduce((acc, lsize) => acc + lsize, 0);

    console.log(`Found ${totalSearches} to be searched for ${anatomicalRegion}:${searchData.searchType} from`, searchedLibraries);
    return {
        ...searchData,
        totalSearches,
        searchedLibraries,
    };
};

const getDataConfig = async (dataBucket) => {
    // dataRefFile is the file that points to the current version.
    // If it is a production environment use 'current.txt' otherwise for dev environments use 'next.txt'
    const dataRefFile = process.env.STAGE.match(/^prod/)
                            ? 'current.txt'
                            : 'next.txt';

    if (DEBUG) console.log(`Get libraries location based on :${dataBucket}:${dataRefFile}`);
    const currentVersionBody = await getS3ContentWithRetry(
        dataBucket,
        dataRefFile
    );
    const currentVersion = currentVersionBody.toString().toString().trim();
    if (DEBUG) console.log(`Current version set to: ${currentVersion}`);
    // get current data configuration
    return await getObjectWithRetry(
        dataBucket,
        `${currentVersion}/config.json`
    );
};

const getAllSearchedLibrariesWithSizes = async (cfgs, libraryNamesGetter) => {
    const searchedLibraries = getAllSearchedLibrariesFromConfigs(cfgs, libraryNamesGetter);
    const getLibraryCountsPromises = await searchedLibraries.map(async libraryConfig => {
        const lsize = await getCount(libraryConfig.libraryBucket, libraryConfig.searchedNeuronsFolder);
        return await {
            ...libraryConfig,
            lsize,
        };
    });
    return await Promise.all(getLibraryCountsPromises);
}

const getAllSearchedLibrariesFromConfigs = (cfgs, libraryNamesGetter) => {
    const lcList = cfgs.flatMap(cfg => libraryNamesGetter(cfg).map(libraryName => {
        const searchedNeuronsFolder = cfg.searchFolder;
        const alignmentSpace = cfg.alignmentSpace;
        // if searchFolder is set append it to <alignmentSpace>/<libraryName>
        const searchedNeuronsPrefix = searchedNeuronsFolder
                                        ? `${alignmentSpace}/${libraryName}/${searchedNeuronsFolder}`
                                        : `${alignmentSpace}/${libraryName}`;

        return new Map({
            libraryBucket: getBucketNameFromURL(cfg.prefixes.ColorDepthMip),
            libraryThumbnailsBucket: getBucketNameFromURL(cfg.prefixes.ColorDepthMipThumbnail),
            alignmentSpace: cfg.alignmentSpace,
            libraryName: libraryName,
            searchedNeuronsFolder: searchedNeuronsPrefix,
        });
    }));
    return [...new Set(lcList)].map(lc => lc.toJS());
};

const getBucketNameFromURL = (bucketURL) => {
    const normalizedBucketURL = bucketURL.endsWith('/')
                                    ? bucketURL.slice(0, -1)
                                    : bucketURL;
    return normalizedBucketURL.substring(normalizedBucketURL.lastIndexOf('/') + 1);
};

const getCount = async (libraryBucket, libraryKey) => {
    if (DEBUG) console.log("Get count from:", libraryKey);
    const countMetadata = await getObjectWithRetry(
        libraryBucket,
        `${libraryKey}/counts_denormalized.json`
    );
    return countMetadata.objectCount;
};
