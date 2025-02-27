import { CognitoIdentityProviderClient, ListUsersCommand } from "@aws-sdk/client-cognito-identity-provider";
import { DynamoDBDocumentClient, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const dbDocClient = DynamoDBDocumentClient.from(new DynamoDBClient({
  maxRetries: 3,
  httpOptions: {
    timeout: 5000
  }
}));

function isUserAdmin(token) {
  // get data from event.requestContext.authorizer.jwt
  // if 'cognito:groups' contains neuronbridge-admins then return true
  if (token) {
    const cognitoGroups = token.claims ? token.claims["cognito:groups"] : null;
    if (cognitoGroups && cognitoGroups.includes("neuronbridge-admins")) {
      return true;
    }
  }
  return false;
}

let filterOptIn = optedInUserNames => (user) => {
  if (optedInUserNames.includes(user.Username)) {
    return true;
  }
  return false;
};

async function getAllEmailAddresses(jwt) {
  if (isUserAdmin(jwt)) {
    console.log("getting list of email addresses");

    const params = {
      TableName: process.env.TABLE,
      ExpressionAttributeNames: {
        "#UN": "username",
        "#ML": "mailingList"
      },
      ProjectionExpression: "#UN, #ML",
      ReturnConsumedCapacity: "TOTAL"
      // TODO: add the filter to only fetch the users that had mailingList
      // set to true
    };

    let lastEvaluatedKey;
    const optedInUserNames = [];

    do {
      const data = await dbDocClient.send(new ScanCommand(params));
      console.log(`Checked ${data.Items.length} preferences records`);
      data.Items.forEach(item => {
        if (item.mailingList) {
          optedInUserNames.push(item.username);
        }
      });
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      lastEvaluatedKey = data.LastEvaluatedKey;
    } while (typeof lastEvaluatedKey !== "undefined");

    // now that we have a list of usernames, we need to convert them to email
    // addresses by pulling all the users from cognito and matching.
    //
    const cognitoUsers = [];

    const identityClient = new CognitoIdentityProviderClient({
      region: "us-east-1"
    });
    const cognitoParams = {
      UserPoolId: process.env.COGNITOPOOLID
    };

    const result = await identityClient.send(new ListUsersCommand(cognitoParams));
    result.Users.filter(filterOptIn(optedInUserNames)).forEach(user => {
      const email = user.Attributes.filter(
        attribute => attribute.Name === "email"
      );
      cognitoUsers.push(email[0].Value);
    });
    console.log(`Checked ${result.Users.length} cognito users`);

    let nextPage = result.PaginationToken;

    while (nextPage) {
      params.PaginationToken = nextPage;
      const nextResult = await identityClient.send(new ListUsersCommand(cognitoParams));
      console.log(`Checked ${nextResult.Users.length} cognito users`);
      nextResult.Users.filter(filterOptIn(optedInUserNames)).forEach(user => {
        const email = user.Attributes.filter(
          attribute => attribute.Name === "email"
        );
        cognitoUsers.push(email[0].Value);
      });

      nextPage = nextResult.PaginationToken;
    }

    return cognitoUsers;
  }
  return [];
}

export const handler = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = { jwt: event.requestContext.authorizer.jwt, event };

  const requestMethod = event.requestContext?.http?.method;
  const jwt = event.requestContext?.authorizer?.jwt || {
    claims: null,
    scopes: null
  };
  // decide if this is a GET, POST or DELETE action.
  // if POST or DELETE, then check the user is in the admins group
  switch (requestMethod) {
    case "GET":
      returnBody = await getAllEmailAddresses(jwt);
      break;
    default:
      returnBody = { req: "unknown", method: requestMethod };
  }

  returnObj.body = JSON.stringify(returnBody);
  return returnObj;
};
