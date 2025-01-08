import {
    DEBUG,
    getObjectWithRetry,
    getS3ContentWithRetry,
    getBucketNameFromURL,
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
 * @param dataBucket
 */
export const getSearchedLibraries = async (searchData, dataBucket) => {
    const anatomicalRegion = searchData.anatomicalRegion || 'Brain';
    console.log(`Getting search libraries for ${anatomicalRegion}:${searchData.searchType}`);
    const dataConfig = await getDataConfig(dataBucket);
    // find all enabled datasets for the specified anatomical area
    const searchCfgs = Object.keys(dataConfig.stores)
        .map(dataStoreKey => {
            const ds = dataConfig.stores[dataStoreKey];
            const anatomicalAreaCfg = dataConfig.anatomicalAreas[ds.anatomicalArea];
            return {
                ...ds,
                store: dataStoreKey,
                alignmentSpace: anatomicalAreaCfg.alignmentSpace,
            };
        })
        .filter(ds => {
            const enabled = ds.customSearch !== undefined;
            return enabled && ds.anatomicalArea.toLowerCase() === anatomicalRegion.toLowerCase();
        });
    if (!searchCfgs) {
        console.error(`No CDS configuration found for ${anatomicalRegion}:${searchData.searchType} in`, dataConfig);
        return {
            ...searchData,
            totalSearches: 0,
            searchedLibraries,
        };
    }
    const searchType = searchData.searchType;
    if (searchData.selectedLibraries) {
        const userSelectedLibraries = new Set(searchData.selectedLibraries);
        librariesWithTypeGetter = cfg => {
            const selectedEMs = cfg.customSearch.emLibraries
                .filter(library => userSelectedLibraries.has(library.name))
                .map(library => {
                    return {
                        ...library,
                        targetType: 'EMImage',
                        searchFolder: cfg.customSearch.searchFolder,
                    };
                });
            const selectedLMs = cfg.customSearch.lmLibraries
                .filter(library => userSelectedLibraries.has(library.name))
                .map(library => {
                    return {
                        ...library,
                        targetType: 'LMImage',
                        searchFolder: cfg.customSearch.searchFolder,
                    };
                });
            console.log('Selected EMs', selectedEMs);
            console.log('Selected LMs', selectedLMs);
            const r = selectedEMs.concat(selectedLMs);
            console.log('Selected LIBS', JSON.stringify(r, null, 4));
            return r;
        };
    } else if (searchType === 'em2lm' || searchType === 'lmTarget') {
        // from all matching configurations collect 'lmLibraries' together with alignmentSpace and bucket
        librariesWithTypeGetter = cfg => cfg.customSearch.lmLibraries.map(library => {
            return {
                ...library,
                targetType: 'LMImage',
                searchFolder: cfg.customSearch.searchFolder,
            };
        });
    } else if (searchType === 'lm2em' || searchType === 'emTarget') {
        // from all matching configurations collect 'emLibraries' together with alignmentSpace and bucket
        librariesWithTypeGetter = cfg => cfg.customSearch.emLibraries.map(library => {
            return {
                ...library,
                targetType: 'EMImage',
                searchFolder: cfg.customSearch.searchFolder,
            };
        });
    } else {
        console.error(`Unsupported search: ${searchType}`, searchData);
        librariesWithTypeGetter = () => [];
    }
    const searchedLibraries = getAllSearchedLibrariesFromConfiguredStores(searchCfgs, librariesWithTypeGetter);
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

const getAllSearchedLibrariesFromConfiguredStores = (dataStores, librariesWithTypeGetter) => {
    const lcList = dataStores.flatMap(dataStore => librariesWithTypeGetter(dataStore).map(library => {
        const libraryName = library.name;
        const publishedNamePrefix = library.publishedNamePrefix;
        const searchedNeuronsFolder = library.searchFolder;
        const alignmentSpace = dataStore.alignmentSpace;
        // if searchFolder is set append it to <alignmentSpace>/<libraryName>
        const searchedNeuronsPrefix = searchedNeuronsFolder
            ? `${alignmentSpace}/${libraryName}/${searchedNeuronsFolder}`
            : `${alignmentSpace}/${libraryName}`;

        console.log('Get target library from', JSON.stringify(dataStore, null, 4));
        return new Map({
            store: dataStore.store,
            anatomicalArea: dataStore.anatomicalArea,
            libraryBucket: getBucketNameFromURL(dataStore.prefixes.CDM),
            libraryThumbnailsBucket: getBucketNameFromURL(dataStore.prefixes.CDMThumbnail),
            alignmentSpace: dataStore.alignmentSpace,
            libraryName: libraryName,
            publishedNamePrefix: publishedNamePrefix,
            searchedNeuronsFolder: searchedNeuronsPrefix,
            lsize: library.count,
            targetType: library.targetType,
        });
    }));
    return [...new Set(lcList)].map(lc => lc.toJS());
};
