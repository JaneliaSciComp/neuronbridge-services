import AWS from "aws-sdk";

const db = new AWS.DynamoDB.DocumentClient({
  maxRetries: 3,
  httpOptions: {
    timeout: 5000
  }
});


export function isUserAdmin(token) {
  // get data from event.requestContext.authorizer.jwt
  // if 'cognito:groups' contains neuronbridge-admins then return true
  if (token) {
    const cognitoGroups = token.claims ? token.claims['cognito:groups'] : null;
    if (cognitoGroups && cognitoGroups.includes('neuronbridge-admins')) {
      return true;
    }
  }
  return false;
}

async function getAnnouncements(args) {
  const defaults = {
    active: "true",
    date: Date.now().toString()
  };

  const { date, active } = {...defaults, ...args};
  // default response is to get active records that start before
  // and end after the time now.
  const params = {
    TableName: process.env.TABLE,
    KeyConditionExpression: "active = :active",
    ExpressionAttributeValues: {
      ":active": active
    },
    ReturnConsumedCapacity: 'TOTAL'
  };

  if (date !== "all") {
    params.FilterExpression = "endTime >= :num and :num >= startTime";
    params.ExpressionAttributeValues[":num"] = date;
  }

  console.log(params);

  let lastEvaluatedKey;
  const foundItems = [];

  do {
    const data = await db.query(params).promise();
    data.Items.forEach(item => foundItems.push(item));
    params.ExclusiveStartKey = data.LastEvaluatedKey;
    lastEvaluatedKey = data.LastEvaluatedKey;
  } while (typeof lastEvaluatedKey !== "undefined");

  return foundItems;
}

async function deleteAnnouncement(jwt, args) {
  if (isUserAdmin(jwt)) {
    const params = {
      TableName: process.env.TABLE,
      Key: {
        'active': 'true',
        'createdTime': args.createdTime
      }
    };
    const result = await db.delete(params).promise();
    return result;
  }
  return {};
}

async function createAnnouncement(jwt, itemAttributes) {
  const now = new Date();
  const defaultEnd = new Date(now).setDate(now.getDate() + 5);

  const item = {
    'active': 'true',
    'startTime': now.getTime().toString(),
    'endTime': defaultEnd.toString(),
    'message': '',
    'closable': true,
    'type': 'info',
    'stamp': false,
    // 'actionText': '', // if not set in the passed in attributes,
    // 'actionLink': '', // then not needed in the record
    ...itemAttributes,
    'createdTime': now.getTime().toString(),
  };

  const params = {
    TableName: process.env.TABLE,
    Item: item
  };
  console.log(params);
  if (isUserAdmin(jwt)) {
    // create entry in table
    const result = await db.put(params).promise();
    return result;
  }
  return {};
}

export const announcements = async event => {
  // console.log(event, db, process.env.TABLE, event.requestContext.authorizer.jwt);
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  const requestMethod = event.requestContext?.http?.method;
  const { queryStringParameters, pathParameters } = event;
  const jwt = event.requestContext?.authorizer?.jwt || { claims: null, scopes: null};
   // decide if this is a GET, POST or DELETE action.
  // if POST or DELETE, then check the user is in the admins group
  switch (requestMethod) {
    case 'GET':
      returnBody = await getAnnouncements(queryStringParameters);
      break;
    case 'POST': {
      let body = {};
      try {
        body = JSON.parse(event.body);
      } catch (error) {
        returnObj.statusCode = 500;
        returnBody.message = error.message;
      }
      returnBody = await createAnnouncement(jwt, body);
      break;
    }
    case 'DELETE':
      returnBody = await deleteAnnouncement(jwt, pathParameters);
      break;
    default:
      returnBody = {req: 'unknown', method: requestMethod};
  }

  returnObj.body = JSON.stringify(returnBody);
  return returnObj;
};
