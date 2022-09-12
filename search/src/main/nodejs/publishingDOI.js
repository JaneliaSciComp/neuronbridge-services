import AWS from "aws-sdk";

const db = new AWS.DynamoDB.DocumentClient({
  maxRetries: 3,
  httpOptions: {
    timeout: 5000
  }
});

async function getDOIs(jwt, query) {
  const params = {
    TableName: process.env.NAMES_TABLE,
    KeyConditionExpression: "#name = :name",
    ExpressionAttributeNames: { "#name": "name" },
    ExpressionAttributeValues: {
      ":name": query
    },
    ReturnConsumedCapacity: 'TOTAL'
  };

  const data = await db.query(params).promise();
  return data.Items[0] || {};
}

export const publishingDOI = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {jwt: event.requestContext.authorizer.jwt, event};

  if (!event.queryStringParameters || !event.queryStringParameters.q) {
    return {
      isBase64Encoded: false,
      statusCode: 400,
      body: JSON.stringify({error: 'Missing query string. eg /publishing_doi?q=1234'})
    };
  }


  const { q: query } = event.queryStringParameters;

  const requestMethod = event.requestContext?.http?.method;
  const jwt = event.requestContext?.authorizer?.jwt || { claims: null, scopes: null};
  // if POST or DELETE, then check the user is in the admins group
  switch (requestMethod) {
    case 'GET':
      returnBody = await getDOIs(jwt, query);
      break;
    default:
      returnBody = {req: 'unknown', method: requestMethod};
  }

  returnObj.body = JSON.stringify(returnBody);
  return returnObj;
};
