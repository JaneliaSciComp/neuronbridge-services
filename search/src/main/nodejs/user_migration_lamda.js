/* migrate users from the original user pool, to the
 * current user pool, on either successful login or after
 * a request to reset their password. */

import { CognitoIdentityProviderClient, AdminInitiateAuthCommand,
         AdminGetUserCommand } from "@aws-sdk/client-cognito-identity-provider";
const identityProviderClient = new CognitoIdentityProviderClient();

const OLD_CLIENT_ID = process.env.OLD_CLIENT_ID;
const OLD_USER_POOL_ID = process.env.OLD_USER_POOL_ID;

async function authenticateUser(username, password) {

  // validate supplied username & password
  const resAuth = await identityProviderClient
    .send(new AdminInitiateAuthCommand({
      AuthFlow: "ADMIN_USER_PASSWORD_AUTH",
      AuthParameters: {
        PASSWORD: password,
        USERNAME: username
      },
      ClientId: OLD_CLIENT_ID,
      UserPoolId: OLD_USER_POOL_ID
    }));
  if (resAuth.code && resAuth.message) {
    return undefined;
  }

  // Load user data
  const resGet = await identityProviderClient
    .send(new AdminGetUserCommand({
      UserPoolId: OLD_USER_POOL_ID,
      Username: username
    }));
  if (resGet.code && resGet.message) {
    return undefined;
  }

  return {
    emailAddress: resGet.UserAttributes.find(e => e.Name === "email").Value,
    sub: resGet.UserAttributes.find(e => e.Name === "sub").Value
  };
}

async function lookupUser(username) {
  try {
    const params = {
      UserPoolId: OLD_USER_POOL_ID,
      Username: username,
    };
    const resGet = await identityProviderClient.send(new AdminGetUserCommand(params));
    if (resGet.code && resGet.message) {
      return undefined;
    }
    return {
      emailAddress: resGet.UserAttributes.find(e => e.Name === "email").Value,
      sub: resGet.UserAttributes.find(e => e.Name === "sub").Value
    };
  } catch (err) {
      console.log(`lookupUser: error ${JSON.stringify(err)}`);
    return undefined;
  }
}

export const userMigration = async (event, context, callback) => {
  if (event.triggerSource === "UserMigration_Authentication") {
    console.log(`migrating user: ${event.userName}`);
    const user = await authenticateUser(event.userName, event.request.password);

    if (user) {

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
  } else if (event.triggerSource === "UserMigration_ForgotPassword") {
    console.log(`migrating user: ${event.userName}`);
    const user = await lookupUser(event.userName);
    if (user) {
      console.log(user);
      event.response.userAttributes = {
        email: user.emailAddress,
        email_verified: "true",
        "custom:migrated": "true"
      };
      event.response.messageAction = "SUPPRESS";
      context.succeed(event);
    } else {
      callback("Username/client id combination not found.");
    }
  } else {
    // Return error to Amazon Cognito
    callback("Bad triggerSource " + event.triggerSource);
  }
};
