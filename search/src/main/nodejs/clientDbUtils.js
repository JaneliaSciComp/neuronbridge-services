import AWS from 'aws-sdk';

var docClient = new AWS.DynamoDB.DocumentClient();

export const queryDb = async params => {
    return await docClient.query(params).promise();
};
