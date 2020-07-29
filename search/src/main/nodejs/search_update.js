'use strict';

const AWS = require("aws-sdk");
const AUTH_TYPE = require("aws-appsync").AUTH_TYPE;
const AWSAppSyncClient = require("aws-appsync").default;
const gql = require("graphql-tag");
require("isomorphic-fetch");

const config = {
    url: process.env.APPSYNC_ENDPOINT,
    region: process.env.AWS_REGION,
    auth: {
        type: AUTH_TYPE.AWS_IAM,
        credentials: AWS.config.credentials
    },
    disableOffline: true
};

const updateSearchMutation = `mutation updateSearch($input: UpdateSearchInput!) {
  updateSearch(input: $input) {
    id
    step
    updatedOn
  }
}`;

const listSearches = `query listSearches {
  listSearches {
    items {
      id
      step
      owner
      algorithm
      createdOn
      updatedOn
    }
  }
}`;

const client = new AWSAppSyncClient(config);

exports.searchUpdate = async (event, context, callback) => {
    const searchDetails = {
        id: event.searchId,
        step: event.step
    };
    console.log(config, searchDetails);

    try {
        const result = await client.mutate({
            mutation: gql(updateSearchMutation),
            variables: { input: searchDetails }
        });
        console.log(result.data);
        callback(null, result.data);
    } catch (e) {
        console.warn("Error sending mutation: ", e);
        callback(Error(e));
    }

    return;
};
