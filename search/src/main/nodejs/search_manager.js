'use strict';

const AWS = require("aws-sdk");

const getSearch = (id) => {
    return {
        id: "IO"
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

const createSearch = (searchParams) => {
    console.log("!!!! CREATE SEARCH ", searchParams);
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

    switch (event.action) {
        case 'getSearch': {
            console.log("!!!!! GET SEARCH ACTION");
            return getSearch(event.arguments.id);
        }
        case 'listSearches': {
            console.log("!!!!! LIST SEARCHES ACTION");
            const {filter, limit, nextToken} = event.arguments;
            console.log("!!!!! LIST SEARCHES ACTION 2", filter, limit, nextToken);
            return listSearches(filter, limit, nextToken);
        }
        case 'createSearch': {
            console.log("!!!!! CREATE SEARCH ACTION");
            const {searchParams} = event.arguments;
            return createSearch(searchParams);
        }
        case 'deleteSearch': {
            console.log("!!!!! DELETE SEARCH ACTION");
            const {searchParams} = event.arguments;
            return deleteSearch(searchParams);
        }
        case 'updateSearch': {
            console.log("!!!!! UPDATE SEARCH ACTION");
            const {searchParams} = event.arguments;
            return updateSearch(searchParams);
        }
        default: {
            return `Unknown action, unable to resolve ${event.action}`;
        }
    }
};
