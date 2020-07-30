'use strict';

const AWS = require("aws-sdk");
const { v1: uuidv1 } = require('uuid');
const moment = require('moment');

const SEARCH_TABLE = process.env.SEARCH_TABLE;

AWS.config.update({
    apiVersion: '2012-08-10'
});

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

const listSearches = async (filter, limit, nextToken) => {
    console.log('List Searches', filter, limit, nextToken);
    const filterValues = {};
    const params = {
        TableName: `${SEARCH_TABLE}`,
        ExpressionAttributeValues: filterValues
    };
    console.log('QueryItems', params);
    const searchData = await dynamoDocClient.query(params).promise();
    console.log('Found searches', searchData);
    return searchData;
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
    const params = {
        TableName: `${SEARCH_TABLE}`,
        Item: searchItem
    };
    console.log('PutItem', params);
    const newSearch = await dynamoDocClient.put(params).promise();
    console.log('Created search', newSearch);
    return params.Item;
}

const deleteSearch = (searchParams) => {
    console.log("!!!! DELETE SEARCH ", searchParams);
}

const updateSearch = async (searchParams) => {
    console.log('Update Search', searchParams);
    const expression = [];
    const updatedValues = {};
    if (searchParams.hasOwnProperty('step')) {
        expression.push('step=:step');
        updatedValues[':step'] = searchParams.step;
    }
    if (searchParams.hasOwnProperty('nBatches')) {
        expression.push('nBatches=:nBatches');
        updatedValues[':nBatches'] = searchParams.nBatches;
    }
    if (searchParams.hasOwnProperty('completedBatches')) {
        expression.push('completedBatches=:completedBatches');
        updatedValues[':completedBatches'] = searchParams.completedBatches;
    }
    if (searchParams.hasOwnProperty('cdsStarted')) {
        expression.push('cdsStarted=:cdsStarted');
        updatedValues[':cdsStarted'] = searchParams.cdsStarted;
    }
    if (searchParams.hasOwnProperty('cdsFinished')) {
        expression.push('cdsFinished=:cdsFinished');
        updatedValues[':cdsFinished'] = searchParams.cdsFinished;
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
        ExpressionAttributeValues: updatedValues,
        ReturnValues: 'ALL_NEW'
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
