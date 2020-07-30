'use strict';

const AWS = require("aws-sdk");
const { v1: uuidv1 } = require('uuid');
const moment = require('moment');

const SEARCH_TABLE = process.env.SEARCH_TABLE;

AWS.config.update({
    apiVersion: '2012-08-10'
});

const dynamoDB = new AWS.DynamoDB();
const dynamoDocClient = new AWS.DynamoDB.DocumentClient();

const getArgs = (e) => { return e.source === 'graphql' ? e.arguments : e; }

const getSearch = async (id) => {
    console.log('Get Search', id);
    const params = {
        TableName: `${SEARCH_TABLE}`,
        Key: {
            'id': id
        }
    };
    console.log('GetItem', params);
    const searchData = await dynamoDocClient.get(params).promise();
    console.log('Found search', searchData);
    return searchData ? searchData.Item : null;
}

const listSearches = (filter, limit, nextToken) => {
    return {
        items: [
            {
                id: "IO"
            }
        ],
        nextToken: "keepGoing"
    }
}

const createSearch = async (searchParams) => {
    console.log('Create Search', searchParams);
    const searchId = uuidv1();
    const now = new Date();
    const inputTypeName = 'Search';
    const searchTimestamp = now.toISOString();
    const searchDir = `/private/${searchParams.identityId}/${searchParams.searchDir}`;
    const step = 0;
    const searchItem = {
        __typename: {S: inputTypeName},
        id: {S: searchId},
        step: {N: ''+step},
        createdOn: {S: searchTimestamp},
        updatedOn: {S: searchTimestamp},
        owner: {S: searchParams.owner},
        identityId: {S: searchParams.identityId},
        searchType: {S: searchParams.searchType},
        searchDir: {S: searchDir},
        upload: {S: searchParams.upload},
        algorithm: {S: searchParams.algorithm},
        mimeType: {S: searchParams.mimeType},
    };
    const params = {
        TableName: `${SEARCH_TABLE}`,
        Item: searchItem
    };
    console.log('PutItem', params);
    const newSearch = await dynamoDB.putItem(params).promise();
    console.log('Created search', newSearch);
    return {
        __typename: inputTypeName,
        id: searchId,
        step: step,
        createdOn: searchTimestamp,
        updatedOn: searchTimestamp,
        owner: searchParams.owner,
        identityId: searchParams.identityId,
        searchType: searchParams.searchType,
        searchDir: searchDir,
        upload: searchParams.upload,
        algorithm: searchParams.algorithm,
        mimeType: searchParams.mimeType
    };
}

const deleteSearch = (searchParams) => {
    console.log("!!!! DELETE SEARCH ", searchParams);
}

const updateSearch = async (searchParams) => {
    console.log('Update Search', searchParams);
    const expression = [];
    const values = {};
    if (searchParams.hasOwnProperty('step')) {
        expression.push('step=:step');
        values[':step'] = searchParams.step;
    }
    if (searchParams.hasOwnProperty('nBatches')) {
        expression.push('nBatches=:nBatches');
        values[':nBatches'] = searchParams.nBatches;
    }
    if (searchParams.hasOwnProperty('completedBatches')) {
        expression.push('completedBatches=:completedBatches');
        values[':completedBatches'] = searchParams.completedBatches;
    }
    if (searchParams.hasOwnProperty('cdsStarted')) {
        expression.push('cdsStarted=:cdsStarted');
        values[':cdsStarted'] = searchParams.cdsStarted;
    }
    if (searchParams.hasOwnProperty('cdsFinished')) {
        expression.push('cdsFinished=:cdsFinished');
        values[':cdsFinished'] = searchParams.cdsFinished;
    }
    if (expression.length === 0) {
        console.log(`No update parameters provided for search ${searchParams.id}`);
        return null;
    }
    const params = {
        TableName: `${SEARCH_TABLE}`,
        Key: {
            'id': searchParams.id
        },
        UpdateExpression: 'set ' + expression.join(','),
        ExpressionAttributeValues: values
    };
    console.log('UpdateItem', params);
    const searchData = await dynamoDocClient.update(params).promise();
    console.log('Updated search', searchData);
    return searchData ? searchData.Item : null;
}

exports.searchManager = async (event) => {
    console.log(event);

    const action = event.action;
    const args = getArgs(event);
    switch (action) {
        case 'getSearch': {
            return await getSearch(args.id);
        }
        case 'listSearches': {
            const {filter, limit, nextToken} = args;
            return listSearches(filter, limit, nextToken);
        }
        case 'createSearch': {
            const searchParams = args.input;
            return await createSearch(searchParams);
        }
        case 'deleteSearch': {
            const searchParams = args.input;
            return deleteSearch(searchParams);
        }
        case 'updateSearch': {
            const searchParams = args.input;
            return updateSearch(searchParams);
        }
        default: {
            return `Unknown action, unable to resolve ${action}`;
        }
    }
};
