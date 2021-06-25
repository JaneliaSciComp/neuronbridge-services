import AWS from "aws-sdk";
import { getOldSubs, searchesToMigrate } from "./utils";

const db = new AWS.DynamoDB.DocumentClient();
const s3 = new AWS.S3();

// save data to new dynamodb table
async function saveSearchToDynamoDB(item, TableName) {
  const params = {
    TableName,
    Item: item
  };

  try {
    await db.put(params).promise();
  } catch (err) {
    console.log(err);
    return err;
  }
}

async function migrateDynamoDB(username, identityId, search) {
  const updatedRecord = { ...search, owner: username, identityId, migrated: true };
  await saveSearchToDynamoDB(updatedRecord, process.env.SEARCH_TABLE);
}

async function migrateS3(identityId, search) {
  const originalPrefix = `private/${search.identityId}/${search.searchDir}/`;
  const newPrefix = `private/${identityId}/${search.searchDir}`;

  const searchFiles = await s3
    .listObjects({
      Bucket: process.env.OLD_SEARCH_BUCKET,
      Prefix: originalPrefix
      // Delimiter: "/"
    })
    .promise();

  // foreach search file create a new path in the new bucket and transfer the
  // file.
  await Promise.all(
    searchFiles.Contents.map(async (fileInfo) => {
      const originalLocation = `${process.env.OLD_SEARCH_BUCKET}/${fileInfo.Key}`;
      const newKey = `${newPrefix}/${fileInfo.Key.replace(originalPrefix, '')}`;
      await s3.copyObject({
        Bucket: process.env.SEARCH_BUCKET,
        CopySource: originalLocation,
        Key: newKey
      }).promise();
    })
  );
}

async function removeDynamoDB(search) {
  const params = {
    TableName: process.env.OLD_SEARCH_TABLE,
    Key: {
      "id": search.id
    }
  };

  try {
    await db.delete(params).promise();
  } catch (err) {
    console.log(`Error removing old dynamoDB entry: ${search.id}`);
    console.log(err);
    return err;
  }

}

export const dataMigration = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  try {
    // get the identityId from the post body
    const { identityId } = JSON.parse(event.body);
    // get the sub from the JWT used to get through the API gateway.
    const { sub, username } = event.requestContext.authorizer.jwt.claims;
    // get old sub by checking email against old user pool.
    const oldSubs = await getOldSubs(username);
    if (oldSubs) {
      // check to see if migration is required.
      const searches = await searchesToMigrate(username, oldSubs);
      await Promise.all(
        searches.map(async search => {
          await migrateDynamoDB(username, identityId, search);
          await migrateS3(identityId, search);
        })
      );
      // remove the old entries from DynamoDB to prevent future migration
      // prompts;
      await Promise.all(
        searches.map(async search => {
          await removeDynamoDB(search);
        })
      );
    }
    console.log("done");
    returnBody = { sub, username, oldData: { oldSubs } };
  } catch (error) {
    console.log(error);
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }
  returnObj.body = JSON.stringify(returnBody);

  return returnObj;
};
