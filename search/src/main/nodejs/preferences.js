import { DynamoDBDocumentClient, DeleteCommand,
         QueryCommand, PutCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const dbDocClient = DynamoDBDocumentClient.from(new DynamoDBClient({
  maxRetries: 3,
  httpOptions: {
    timeout: 5000
  }
}));

async function deletePreferences(jwt) {
  const params = {
    TableName: process.env.TABLE,
    Key: {
      'username': jwt.claims.username
    }
  };
  const result = await dbDocClient.send(new DeleteCommand(params));
  return result;
}

async function getPreferences(jwt) {
  const params = {
    TableName: process.env.TABLE,
    KeyConditionExpression: "username = :username",
    ExpressionAttributeValues: {
      ":username": jwt.claims.username
    },
    ReturnConsumedCapacity: 'TOTAL'
  };

  const data = await dbDocClient.send(new QueryCommand(params));
  return data.Items[0] || {};
}

async function updatePreferences(jwt, itemAttributes) {
  const now = new Date();
  const item = {
    'mailingList': false,
    ...itemAttributes,
    'username': jwt.claims.username,
    'updatedTime': now.getTime().toString(),
  };

  const params = {
    TableName: process.env.TABLE,
    Item: item
  };
  await dbDocClient.send(new PutCommand(params));
  return item;
}

export const handler = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {jwt: event.requestContext.authorizer.jwt, event};

  const requestMethod = event.requestContext?.http?.method;
  // const { queryStringParameters, pathParameters } = event;
  const jwt = event.requestContext?.authorizer?.jwt || { claims: null, scopes: null};
   // decide if this is a GET, POST or DELETE action.
  // if POST or DELETE, then check the user is in the admins group
  switch (requestMethod) {
    case 'GET':
      returnBody = await getPreferences(jwt);
      break;
    case 'POST': {
      let body = {};
      try {
        body = JSON.parse(event.body);
      } catch (error) {
        returnObj.statusCode = 500;
        returnBody.message = error.message;
      }
      returnBody = await updatePreferences(jwt, body);
      break;
    }
    case 'DELETE':
      returnBody = await deletePreferences(jwt);
      break;
    default:
      returnBody = {req: 'unknown', method: requestMethod};
  }

  returnObj.body = JSON.stringify(returnBody);
  return returnObj;
};
