import  { getOldSubs, searchesToMigrate} from "./utils";

export const migrationCheck = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({migrate: false})
  };
  let returnBody = {migrate: false};

  try {
    // get the sub from the JWT used to get through the API gateway.
    const { sub, username } = event.requestContext.authorizer.jwt.claims;
    // get old sub by checking email against old user pool.
    const oldSubs = await getOldSubs(username);
    if (oldSubs) {
      // check to see if migration is required.
      const searches = await searchesToMigrate(sub, oldSubs);
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
