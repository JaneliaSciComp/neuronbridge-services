'use strict';

const AWS = require('aws-sdk');
const AWSAppSyncClient = require("aws-appsync").default;
const AUTH_TYPE = require('aws-appsync').AUTH_TYPE;
const gql = require("graphql-tag");
require("isomorphic-fetch");

const DEBUG = !!process.env.DEBUG;

exports.SEARCH_IN_PROGRESS = 3
exports.SEARCH_COMPLETED = 4

const appSyncClient = new AWSAppSyncClient({
    url: process.env.APPSYNC_API_URL,
    region: process.env.AWS_REGION,
    auth: {
        type: AUTH_TYPE.AWS_IAM,
        credentials: AWS.config.credentials
    },
    disableOffline: true
});

exports.getSearchMetadata = async (searchId) => {
    const result = await appSyncClient.query({
        query: gql(`query getSearch($searchId: ID!) {
            getSearch(id: $searchId) {
                id
                step
                owner
                identityId
                searchDir
                upload
                searchType
                algorithm
                channel
                voxelX
                voxelY
                voxelZ
                nBatches
                completedBatches
                cdsStarted
                cdsFinished
                createdOn
                updatedOn
                displayableMask
                searchMask
                computedMIPs
                errorMessage
            }
        }`),
        variables: { searchId: searchId}
    });
    console.log("Search data for", searchId, result);
    const searchResult = toSearchResult(result.data.getSearch);
    console.log("Found search result", searchResult);
    return searchResult;
}

exports.lookupSearchMetadata = async (searchFilterParams) => {
    let searchFilter = {};
    if (searchFilterParams.currentSearchId) {
        searchFilter.id = {ne: searchFilterParams.currentSearchId}
    }
    if (searchFilterParams.identityId) {
        searchFilter.identityId = {eq: searchFilterParams.identityId}
    }
    if (searchFilterParams.owner) {
        searchFilter.owner = {eq: searchFilterParams.owner}
    }
    if (searchFilterParams.lastUpdated) {
        const lastUpdated = searchFilterParams.lastUpdated;
        lastUpdated.setHours(0,0,0,0);
        searchFilter.updatedOn = {"ge": lastUpdated.toISOString()};
    }
    const result = await appSyncClient.query({
        query: gql(`query listSearches($searchFilter: TableSearchFilterInput!) {
            listSearches(filter: $searchFilter, limit: 100, nextToken: null) {
                items {
                    id
                    step
                    owner
                    identityId
                    searchDir
                    upload
                    searchType
                    algorithm
                    channel
                    voxelX
                    voxelY
                    voxelZ
                    nBatches
                    completedBatches
                    cdsStarted
                    cdsFinished
                    createdOn
                    updatedOn
                    displayableMask
                    searchMask
                    computedMIPs
                    errorMessage
                }
            }
        }`),
        variables: { searchFilter: searchFilter}
    });
    console.log("Search data for", searchFilterParams, result);
    const searches = result.data.listSearches.items
        .map(s => toSearchResult(s))
        .filter(s => searchFilterParams.withNoErrorsOnly ? !s.errorMessage : true);
    console.log("Found searches", searches);
    return searches;
}

exports.updateSearchMetadata = async (searchData) => {
    if (!searchData.id) {
        if (DEBUG) console.log('Update not invoked because no search ID was set');
        return searchData;
    }
    const result = await appSyncClient.mutate({
        mutation: gql(`mutation updateSearch($updateInput: UpdateSearchInput!) {
            updateSearch(input: $updateInput) {
                id
                step
                owner
                identityId
                searchDir
                upload
                searchType
                algorithm
                nBatches
                completedBatches
                cdsStarted
                cdsFinished
                createdOn
                updatedOn
                displayableMask
                searchMask
                computedMIPs
                errorMessage
            }
        }`),
        variables: {
            updateInput: searchData
        }
    });
    console.log("Updated search for", searchData, result);
    const updatedSearch = toSearchResult(result.data.updateSearch);
    console.log("Updated search result", updatedSearch);
    return updatedSearch;
}

const toSearchResult = (searchData) => {
    if (!searchData) {
        return searchData;
    }
    const searchInputFolder = `private/${searchData.identityId}/${searchData.searchDir}`;
    const searchMask = searchData.searchMask
        ? { searchMask: searchData.searchMask, searchInputMask: `${searchInputFolder}/${searchData.searchMask}`}
        : {};
    return {
        searchId: searchData.id,
        searchInputFolder: searchInputFolder,
        searchInputName: `${searchData.upload}`,
        searchInput: `${searchInputFolder}/${searchData.upload}`,
        ...searchMask,
        ...searchData
    }
}
