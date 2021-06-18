import  { getOldSubs, searchesToMigrate} from "./utils";

export const migrationCheck = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({migrate: false})
  };
  let returnBody = {migrate: false};

  try {
    // get the username from the JWT used to get through the API gateway.
    const { username } = event.requestContext.authorizer.jwt.claims;
    // get old username by checking email against old user pool.
    const oldUsernames = await getOldSubs(username);
    if (oldUsernames) {
      // check to see if migration is required.
      const searches = await searchesToMigrate(username, oldUsernames);
      if (searches.length > 0) {
        returnBody = {migrate: true};
      }
    }
  } catch (error) {
    console.log(error);
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }
  returnObj.body = JSON.stringify(returnBody);
  return returnObj;
};
