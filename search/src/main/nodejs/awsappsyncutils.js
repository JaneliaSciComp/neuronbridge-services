import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { Sha256 } from '@aws-crypto/sha256-js';
import { default as fetch, Request } from 'node-fetch';
import { HttpRequest } from '@smithy/protocol-http';

const APPSYNC_API_URL = new URL(process.env.APPSYNC_API_URL);
const DEBUG = !!process.env.DEBUG;

export const ALIGNMENT_JOB_SUBMITTED = 1;
export const ALIGNMENT_JOB_COMPLETED = 2;
export const SEARCH_IN_PROGRESS = 3;
export const SEARCH_COMPLETED = 4;

const getSearchMetadataGQL = `query getSearch($searchId: ID!) {
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
        alignmentMovie
        alignmentScore
    }
}`;

const lookupSearchMetadataGQL = `query listSearches($searchFilter: TableSearchFilterInput!) {
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
            alignmentMovie
            alignmentScore
        }
    }
}`;

const createSearchMetadataGQL = `mutation createSearch($createInput: CreateSearchInput!) {
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
      anatomicalRegion
    }
}`;

const updateSearchMetadataGQL = `mutation updateSearch($updateInput: UpdateSearchInput!) {
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
        anatomicalRegion
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
        alignmentMovie
        alignmentScore
    }
}`;

const credentialsProvider = fromNodeProviderChain({
});

const makeSignedAppSyncRequest = async (gqlString, variables) => {
    try {
        const credentials = await credentialsProvider();
        const signer = new SignatureV4({
            region: process.env.AWS_REGION,
            service: 'appsync',
            sha256: Sha256,
            credentials: credentials,
        });

        const body = JSON.stringify({
            query: gqlString,
            variables,
        });
        console.log(`GQL request - host:${APPSYNC_API_URL.hostname}, `,
                    `path:${APPSYNC_API_URL.path}`,
                    body);
        const request = new HttpRequest({
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'host': APPSYNC_API_URL.hostname,
            },
            hostname: APPSYNC_API_URL.hostname,
            path: APPSYNC_API_URL.pathname,
            body: body,
            region: process.env.AWS_REGION,
        });
        console.log('GQL request before signing it:', request);
        const signedRequest = await signer.sign(request, {
            signingDate: new Date(),
        });
        console.log('Signed GQL request:', signedRequest);
        const httpRequest = new Request(APPSYNC_API_URL, signedRequest);
        const response = await fetch(httpRequest);

        console.log('GQL response:', response);
        return response.json();
    } catch(err) {
        console.error('Error making signed request', gqlString, err);
        throw err;
    }
};


export const getSearchMetadata = async (searchId) => {
    console.log('Searching for', searchId);
    const result = await makeSignedAppSyncRequest(
        getSearchMetadataGQL,
        { searchId: searchId }
    );
    console.log('Search result for', searchId, result);
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
        searchFilter.updatedOn = {'ge': lastUpdated.toISOString()};
    }
    console.log('Searching for', searchFilterParams);
    const result = await makeSignedAppSyncRequest(
        lookupSearchMetadataGQL,
        { searchFilter: searchFilter}
    );
    console.log('Search result for', searchFilterParams, result);
    const searches = result.data.listSearches.items
        .map(s => toSearchResult(s))
        .filter(s => searchFilterParams.withNoErrorsOnly ? !s.errorMessage && !s.alignmentErrorMessage : true);
    console.log("Found searches", searches);
    return searches;
};

export const createSearchMetadata = async (searchData) => {
    console.log('Create search metadata', searchData);
    const result = await makeSignedAppSyncRequest(
        createSearchMetadataGQL,
        { createInput: searchData }
    );
    console.log('Create search metadata result', searchData, result);
    const newSearch = toSearchResult(result.data.createSearch);
    return newSearch;
};

export const updateSearchMetadata = async (searchData) => {
    if (!searchData.id) {
        if (DEBUG) console.log('Update not invoked because no search ID was set');
        return searchData;
    }
    console.log('Update search metadata', searchData);
    const result = await makeSignedAppSyncRequest(
        updateSearchMetadataGQL,
        { updateInput: searchData }
    );
    console.log('Updated search for', searchData, result);
    const updatedSearch = toSearchResult(result.data.updateSearch);
    console.log('Updated search result', updatedSearch);
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
