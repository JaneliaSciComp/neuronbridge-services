import AWS  from 'aws-sdk';
import AWSAppSyncClient from "aws-appsync";
import { AUTH_TYPE } from 'aws-appsync';
import gql from "graphql-tag";
require("isomorphic-fetch");

const DEBUG = !!process.env.DEBUG;

export const ALIGNMENT_JOB_SUBMITTED = 1;
export const ALIGNMENT_JOB_COMPLETED = 2;
export const SEARCH_IN_PROGRESS = 3;
export const SEARCH_COMPLETED = 4;

const appSyncClient = new AWSAppSyncClient({
    url: process.env.APPSYNC_API_URL,
    region: process.env.AWS_REGION,
    auth: {
        type: AUTH_TYPE.AWS_IAM,
        credentials: AWS.config.credentials
    },
    disableOffline: true
});

export const getSearchMetadata = async (searchId) => {
    const result = await appSyncClient.query({
        query: gql(`query getSearch($searchId: ID!) {
            getSearch(id: $searchId) {
                id
                step
                owner
                identityId
                searchDir
                upload
                uploadThumbnail
                searchType
                anatomicalRegion
                algorithm
                userDefinedImageParams
                channel
                referenceChannel
                voxelX
                voxelY
                voxelZ
                maskThreshold
                dataThreshold
                pixColorFluctuation
                xyShift
                mirrorMask
                minMatchingPixRatio
                maxResultsPerMask
                nBatches
                completedBatches
                nTotalMatches
                cdsStarted
                cdsFinished
                alignStarted
                alignFinished
                alignmentSize
                createdOn
                updatedOn
                displayableMask
                searchMask
                computedMIPs
                errorMessage
                alignmentErrorMessage
                simulateMIPGeneration
            }
        }`),
        variables: { searchId: searchId},
        fetchPolicy: 'no-cache'
    });
    console.log("Search data for", searchId, result);
    const searchResult = toSearchResult(result.data.getSearch);
    console.log("Found search result", searchResult);
    return searchResult;
};

export const lookupSearchMetadata = async (searchFilterParams) => {
    let searchFilter = {};
    if (searchFilterParams.currentSearchId) {
        searchFilter.id = {ne: searchFilterParams.currentSearchId};
    }
    if (searchFilterParams.identityId) {
        searchFilter.identityId = {eq: searchFilterParams.identityId};
    }
    if (searchFilterParams.owner) {
        searchFilter.owner = {eq: searchFilterParams.owner};
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
                    uploadThumbnail
                    searchType
                    algorithm
                    userDefinedImageParams
                    channel
                    referenceChannel
                    voxelX
                    voxelY
                    voxelZ
                    maskThreshold
                    dataThreshold
                    pixColorFluctuation
                    xyShift
                    mirrorMask
                    minMatchingPixRatio
                    maxResultsPerMask
                    nBatches
                    completedBatches
                    nTotalMatches
                    cdsStarted
                    cdsFinished
                    alignStarted
                    alignFinished
                    alignmentSize
                    createdOn
                    updatedOn
                    displayableMask
                    searchMask
                    computedMIPs
                    errorMessage
                    alignmentErrorMessage
                    simulateMIPGeneration
                }
            }
        }`),
        variables: { searchFilter: searchFilter}
    });
    console.log("Search data for", searchFilterParams, result);
    const searches = result.data.listSearches.items
        .map(s => toSearchResult(s))
        .filter(s => searchFilterParams.withNoErrorsOnly ? !s.errorMessage && !s.alignmentErrorMessage : true);
    console.log("Found searches", searches);
    return searches;
};

export const createSearchMetadata = async (searchData) => {
  const result = await appSyncClient.mutate({
    mutation: gql(`mutation createSearch($createInput: CreateSearchInput!) {
      createSearch(input: $createInput) {
        id
        step
        owner
        identityId
        searchDir
        upload
        uploadThumbnail
        searchType
        algorithm
        maskThreshold
        dataThreshold
        pixColorFluctuation
        xyShift
        mirrorMask
        minMatchingPixRatio
        maxResultsPerMask
        nBatches
        completedBatches
        nTotalMatches
        cdsStarted
        cdsFinished
        alignStarted
        alignFinished
        createdOn
        updatedOn
        displayableMask
        searchMask
        computedMIPs
        errorMessage
        alignmentErrorMessage
      }
    }`),
    variables: {
      createInput: searchData
    }
  });
  const newSearch = toSearchResult(result.data.createSearch);
  return newSearch;
};

export const updateSearchMetadata = async (searchData) => {
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
                uploadThumbnail
                searchType
                algorithm
                maskThreshold
                dataThreshold
                pixColorFluctuation
                xyShift
                mirrorMask
                minMatchingPixRatio
                maxResultsPerMask
                nBatches
                completedBatches
                nTotalMatches
                cdsStarted
                cdsFinished
                alignStarted
                alignFinished
                alignmentSize
                createdOn
                updatedOn
                displayableMask
                searchMask
                computedMIPs
                errorMessage
                alignmentErrorMessage
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
};

const toSearchResult = (searchData) => {
    if (!searchData) {
        return searchData;
    }
    const searchInputFolder = `private/${searchData.identityId}/${searchData.searchDir}`;
    const searchMask = searchData.searchMask
        ? { searchMask: searchData.searchMask,
            searchInputMask: `${searchInputFolder}/${searchData.searchMask}`
          }
        : {};
    return {
        searchId: searchData.id,
        searchInputFolder: searchInputFolder,
        searchInputName: `${searchData.upload}`,
        ...searchMask,
        ...searchData
    };
};
