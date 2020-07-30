'use strict';

const AWS = require("aws-sdk");
const { v1: uuidv1 } = require('uuid');
const moment = require('moment');

const SEARCH_TABLE = process.env.SEARCH_TABLE;

AWS.config.update({
    apiVersion: '2012-08-10'
});

const dynamoDB = new AWS.DynamoDB();

const getArgs = (e) => { return e.source === 'graphql' ? e.arguments : e; }

const getSearch = (id) => {
    return {
        id: id,
        algorithm: "max"
    }
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
    console.log("!!!! CREATE SEARCH ", searchParams);
    const searchId = uuidv1();
    const now = new Date();
    const searchTimestamp = now.toISOString();
    const searchItem = {
        id: {S: searchId},
        __typename: {S: 'Search'},
        step: {N: "0"},
        createdOn: {S: searchTimestamp},
        updatedOn: {S: searchTimestamp},
        owner: {S: searchParams.owner},
        identityId: {S: searchParams.identityId},
        searchType: {S: searchParams.searchType},
        searchDir: {S: searchParams.searchDir},
        upload: {S: searchParams.upload},
        algorithm: {S: searchParams.algorithm},
        mimeType: {S: searchParams.mimeType},
    };
    const params = {
        Item: searchItem,
        TableName: `${SEARCH_TABLE}`
    };
    console.log("PutItem", params);
    const newSearch = await dynamoDB.putItem(params).promise();
    console.log("Created search", newSearch);
    return {
        id: searchId,
        step: 0,
        createdOn: searchTimestamp,
        updatedOn: searchTimestamp,
        ...searchParams
    };
}

const deleteSearch = (searchParams) => {
    console.log("!!!! DELETE SEARCH ", searchParams);
}

const updateSearch = (searchParams) => {
    console.log("!!!! UPDATE SEARCH ", searchParams);
}

exports.searchManager = async (event) => {
    console.log(event);

    const action = event.action;
    const args = getArgs(event);
    switch (action) {
        case 'getSearch': {
            console.log("!!!!! GET SEARCH ACTION");
            return getSearch(args.id);
        }
        case 'listSearches': {
            console.log("!!!!! LIST SEARCHES ACTION");
            const {filter, limit, nextToken} = args;
            console.log("!!!!! LIST SEARCHES ACTION 2", filter, limit, nextToken);
            return listSearches(filter, limit, nextToken);
        }
        case 'createSearch': {
            console.log("!!!!! CREATE SEARCH ACTION");
            const searchParams = args.input;
            return await createSearch(searchParams);
        }
        case 'deleteSearch': {
            console.log("!!!!! DELETE SEARCH ACTION");
            const searchParams = args.input;
            return deleteSearch(searchParams);
        }
        case 'updateSearch': {
            console.log("!!!!! UPDATE SEARCH ACTION");
            const searchParams = args.input;
            return updateSearch(searchParams);
        }
        default: {
            return `Unknown action, unable to resolve ${action}`;
        }
    }
};
