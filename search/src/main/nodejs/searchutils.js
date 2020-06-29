'use strict'

exports.getSearchParamsKey = (searchInputKey) =>  {
    return `${searchInputKey}.search`;
}

exports.getSearchMetadataKey = (searchInputKey) => {
    return `${searchInputKey}.metadata`;
}

exports.getSearchProgressKey = (searchInputKey) => {
    return `${searchInputKey}.progress`;
}

exports.getSearchResultsKey = (searchInputKey) => {
    return `${searchInputKey}.result`;
}

exports.getIntermediateSearchResultsPrefix = (searchInputKey) => {
    const searchInputPathComps = searchInputKey.split('/');
    if (!searchInputPathComps.length) {
        return `results`;
    } else {
        return searchInputPathComps.slice(0, -1).join('/')+`/results`;
    }
}

exports.getIntermediateSearchResultsKey = (searchInputKey, batchNumber) => {
    const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(searchInputKey);
    const batchId = 'batch_' + batchNumber.toString().padStart(4,"0") + '.json';
    return `${intermediateSearchResultsPrefix}/${batchId}`;
}
