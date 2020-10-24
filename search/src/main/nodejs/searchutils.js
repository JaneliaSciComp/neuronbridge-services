'use strict'

// Strips extension from filepath, e.g.
// mask5876558744700983093.png -> mask5876558744700983093
const getKeyWithNoExt = (searchInputKey) => {
    const extSeparatorIndex = searchInputKey.lastIndexOf('.');
    return extSeparatorIndex > 0 ? searchInputKey.substring(0, extSeparatorIndex) : searchInputKey;
}

// If ext is not provided, the searchInputName is returned.
// Otherwise, the extension on searchInputName is replaced with ext.
const getSearchKey = (searchInputName, ext) => {
    if (!ext) {
        return searchInputName;
    } else {
        const searchInputKey = getKeyWithNoExt(searchInputName);
        return searchInputKey + ext;
    }
}

// Takes the filename (last path element) from the searchInputName and replaces the extension with extParam (if it's not empty)
// Note that extParam is added to the end of the filename without a dot prefix. If you want a dot, you need to specify it in extParam.
const getSearchMaskId = (searchInputName, extParam) => {
    const searchInputKey = getKeyWithNoExt(searchInputName);
    const searchInputPathComps = searchInputKey.split('/');
    const ext = extParam ? extParam : '';
    return searchInputPathComps[searchInputPathComps.length-1] + ext;
}

// Replaces searchInputName's extension (if any) with ".metadata"
const getSearchMetadataKey = (searchInputName) => {
    const searchInputKey = getKeyWithNoExt(searchInputName);
    return `${searchInputKey}.metadata`;
}

// Replaces searchInputName's extension (if any) with ".result"
const getSearchResultsKey = (searchInputName) => {
    const searchInputKey = getKeyWithNoExt(searchInputName);
    return `${searchInputKey}.result`;
}

// If searchInputName contains no path elements, this returns folderName.
// Otherwise, it replaces searchInputName's last path element with folderName.
const getSearchSubFolder = (searchInputName, folderName) => {
    const searchInputKey = getKeyWithNoExt(searchInputName);
    const searchInputPathComps = searchInputKey.split('/');
    if (searchInputPathComps.length > 1) {
        return searchInputPathComps.slice(0, -1).join('/')+`/${folderName}`;
    } else {
        // this happens if the key is in the root folder considering that aws root folder does not start with '/'
        return folderName;
    }
}

// Get the key to results folder relative to the search input
const getIntermediateSearchResultsPrefix = (searchInputName) => getSearchSubFolder(searchInputName, 'results');

// Get the key to results/batch_{batchNumber}.json relative to search input
const getIntermediateSearchResultsKey = (searchInputName, batchNumber) => {
    const intermediateSearchResultsPrefix = getSearchSubFolder(searchInputName, 'results');
    const batchId = 'batch_' + batchNumber.toString().padStart(4,"0") + '.json';
    return `${intermediateSearchResultsPrefix}/${batchId}`;
}

module.exports = {
    getSearchKey,
    getSearchMaskId,
    getSearchMetadataKey,
    getSearchResultsKey,
    getSearchSubFolder,
    getIntermediateSearchResultsPrefix,
    getIntermediateSearchResultsKey
}
