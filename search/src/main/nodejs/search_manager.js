'use strict';

const AWS = require("aws-sdk");
const dynamo = new AWS.DynamoDB.DocumentClient();
const { v1: uuidv1 } = require('uuid');
const moment = require('moment');

const SEARCH_TABLE = process.env.SEARCH_TABLE;

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
    const searchTimestamp = moment(now.toISOString());
    const searchItem = {
        id: searchId,
        step: 0,
        createdOn: searchTimestamp,
        updatedOn: searchTimestamp,
        ...searchParams
    }
    const params = {
        Key: {
            id: searchId
        },
        Item: searchItem,
        TableName: `${SEARCH_TABLE}`,
        ReturnValues: 'ALL_NEW'
    };

    return await dynamo.put(params);

    return {
        id: "new",
        algorithm: "max"
    };
}

const deleteSearch = (searchParams) => {
    console.log("!!!! DELETE SEARCH ", searchParams);
}

const updateSearch = (searchParams) => {
    console.log("!!!! UPDATE SEARCH ", searchParams);
}

exports.searchManager = async (event, context) => {
    console.log(event);
    console.log("!!!!!!!", context);

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
