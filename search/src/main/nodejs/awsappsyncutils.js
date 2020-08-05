'use strict';

const AWS = require('aws-sdk');
const AWSAppSyncClient = require("aws-appsync").default;
const AUTH_TYPE = require('aws-appsync').AUTH_TYPE;
const gql = require("graphql-tag");
require("isomorphic-fetch");

const DEBUG = !!process.env.DEBUG;

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
                nBatches
                completedBatches
                cdsStarted
                cdsFinished
            }
        }`),
        variables: { searchId: searchId}
    });
    const resultData = result.data.getSearch;
    const searchResult = {
        searchId: resultData.id,
        searchInputName: `/private/${resultData.identityId}/${resultData.searchDir}/${resultData.upload}`,
        ...resultData
    }
    console.log("Found search", result, searchResult);
    return searchResult;
}

exports.updateSearchMetadata = async (searchInput) => {
    const result = await appSyncClient.mutate({
        query: gql(`mutation updateSearch($searchInput: UpdateSearchInput!) {
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
            }
        }`),
        variables: {
            searchInput: searchInput
        }
    });
    const resultData = result.data.updateSearch;
    const updatedSearch = {
        searchId: resultData.id,
        searchInputName: `/private/${resultData.identityId}/${resultData.searchDir}/${resultData.upload}`,
        ...resultData
    }
    console.log("Updated search", result, updatedSearch);
    return updatedSearch;
}
