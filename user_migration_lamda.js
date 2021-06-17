const AWS = require("aws-sdk");
const db = new AWS.DynamoDB.DocumentClient();

const OLD_CLIENT_ID = process.env.OLD_CLIENT_ID;
const OLD_USER_POOL_ID = process.env.OLD_USER_POOL_ID;

async function authenticateUser(username, password) {
  const isp = new AWS.CognitoIdentityServiceProvider();

  // validate supplied username & password
  const resAuth = await isp
    .adminInitiateAuth({
      AuthFlow: "ADMIN_USER_PASSWORD_AUTH",
      AuthParameters: {
        PASSWORD: password,
        USERNAME: username
      },
      ClientId: OLD_CLIENT_ID,
      UserPoolId: OLD_USER_POOL_ID
    })
    .promise();
  if (resAuth.code && resAuth.message) {
    return undefined;
  }

  // Load user data
  const resGet = await isp
    .adminGetUser({
      UserPoolId: OLD_USER_POOL_ID,
      Username: username
    })
    .promise();
  if (resGet.code && resGet.message) {
    return undefined;
  }

  console.log(resGet.UserAttributes);

  return {
    emailAddress: resGet.UserAttributes.find(e => e.Name === "email").Value,
    sub: resGet.UserAttributes.find(e => e.Name === "sub").Value
  };
}

//fetch data from original dynamodb table
async function fetchData(ownerId) {
  const params = {
    TableName: "janelia-neuronbridge-search-table-prod",
    FilterExpression: "#owner = :owner",
    ExpressionAttributeValues: {
      ":owner": ownerId
    },
    ExpressionAttributeNames: {
      "#owner": "owner"
    }
  };

  try {
    const data = await db.scan(params).promise();
    return data;
  } catch (err) {
    return err;
  }
}

// save data to new dynamodb table
async function saveNewItem(item) {
  const params = {
    TableName: "janelia-neuronbridge-search-table-dev",
    Item: item
  };

  try {
    await db.put(params).promise();
  } catch (err) {
    console.log(err);
    return err;
  }
}

async function migrate_dynamodb(sub) {
  const originalData = await fetchData(sub);

  for (let record of originalData.Items) {
    const updatedRecord = { ...record, migrated: true };
    try {
      await saveNewItem(updatedRecord);
    } catch (err) {
      console.log(err);
    }
    console.log("saved record to new table");
  }
}

exports.handler = async (event, context, callback) => {
  // TODO implement
  if (event.triggerSource === "UserMigration_Authentication") {
    const user = await authenticateUser(event.userName, event.request.password);

    if (user) {
      console.log("migrating user");
      // migrate dynamodb data
      const result = await migrate_dynamodb(user.sub);

      console.log("adding user to new pool");
      // add user to new user pool
      event.response.userAttributes = {
        email: user.emailAddress,
        email_verified: "true",
        "custom:migrated": "true"
      };
      event.response.finalUserStatus = "CONFIRMED";
      event.response.messageAction = "SUPPRESS";
      context.succeed(event);

      // migrate s3 buckets
    } else {
      callback("Username & password entered was not correct");
    }
  } else {
    // Return error to Amazon Cognito
    callback("Bad triggerSource " + event.triggerSource);
  }
};
