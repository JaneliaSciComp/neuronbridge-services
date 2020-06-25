'use strict'

export const getSearchParamsKey = (searchInputKey) => `${searchInputKey}.search`;

export const getSearchMetadataKey = (searchInputKey) => `${searchInputKey}.metadata`;

export const getSearchProgressKey = (searchInputKey) => `${searchInputKey}.progress`;

export const getSearchResultsKey = (searchInputKey) => `${searchInputKey}.result`;

export const getIntermediateSearchResultsPrefix = (searchInputKey) => {
    const searchInputPathComps = searchInputKey.split('/');
    if (!searchInputPathComps.length) {
        return `results`;
    } else {
        return searchInputPathComps.slice(0, -1).join('/')+`/results`;
    }
}

export const getIntermediateSearchResultsKey = (searchInputKey, batchNumber) => {
    const intermediateSearchResultsPrefix = getIntermediateSearchResultsPrefix(searchInputKey);
    const batchId = 'batch_' + batchNumber.toString().padStart(4,"0") + '.json';
    return `${intermediateSearchResultsPrefix}/${batchId}`;
}
