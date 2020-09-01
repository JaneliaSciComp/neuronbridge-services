'use strict'

const getKeyWithNoExt = (searchInputKey) => {
    const extSeparatorIndex = searchInputKey.lastIndexOf('.');
    return extSeparatorIndex > 0 ? searchInputKey.substring(0, extSeparatorIndex) : searchInputKey;
}

exports.getSearchKey = (searchInputName, ext) => {
    if (!ext) {
        return searchInputName;
    } else {
        const searchInputKey = getKeyWithNoExt(searchInputName);
        return searchInputKey + ext;
    }
}

exports.getSearchMaskId = (searchInputName, extParam) => {
    const searchInputKey = getKeyWithNoExt(searchInputName);
    const searchInputPathComps = searchInputKey.split('/');
    if (!searchInputPathComps.length) {
        return null;
    } else {
        const ext = extParam ? extParam : '';
        return searchInputPathComps[searchInputPathComps.length-1] + ext;
    }
}

exports.getSearchMetadataKey = (searchInputName) => {
    const searchInputKey = getKeyWithNoExt(searchInputName);
    return `${searchInputKey}.metadata`;
}

exports.getSearchResultsKey = (searchInputName) => {
    const searchInputKey = getKeyWithNoExt(searchInputName);
    return `${searchInputKey}.result`;
}

const getIntermediateSearchResultsPrefix = (searchInputName) => {
    const searchInputKey = getKeyWithNoExt(searchInputName);
    const searchInputPathComps = searchInputKey.split('/');
    if (!searchInputPathComps.length) {
        return `results`;
    } else {
        return searchInputPathComps.slice(0, -1).join('/')+`/results`;
    }
}

exports.getIntermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix;

exports.getIntermediateSearchResultsKey = (searchInputName, batchNumber) => {
    const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(searchInputName);
    const batchId = 'batch_' + batchNumber.toString().padStart(4,"0") + '.json';
    return `${intermediateSearchResultsPrefix}/${batchId}`;
}
