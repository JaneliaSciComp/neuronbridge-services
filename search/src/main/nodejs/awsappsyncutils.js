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
                searchMask
                computedMIPs
            }
        }`),
        variables: { searchId: searchId}
    });
    const searchResult = toSearchResult(result.data.getSearch);
    console.log("Found search", result, searchResult);
    return searchResult;
}

exports.updateSearchMetadata = async (searchInput) => {
    const result = await appSyncClient.mutate({
        mutation: gql(`mutation updateSearch($searchInput: UpdateSearchInput!) {
            updateSearch(input: $searchInput) {
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
                searchMask
                computedMIPs
            }
        }`),
        variables: {
            searchInput: searchInput
        }
    });
    const updatedSearch = toSearchResult(result.data.updateSearch);
    console.log("Updated search", result, updatedSearch);
    return updatedSearch;
}
const toSearchResult = (searchData) => {
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
