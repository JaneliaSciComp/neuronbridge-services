import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

var docClient = DynamoDBDocumentClient.from(new DynamoDBClient());

export const queryDb = async params => {
    return await docClient.send(new QueryCommand(params));
};
