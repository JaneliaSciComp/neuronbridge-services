// get all the searches from dynamodb that haven't been migrated
// foreach search
// get the results file for the search from s3.
// copy the .result file to a .result.2.0 file
// modify the results to match the new data format
// save it back to disk as the .result file.

import AWS from "aws-sdk";
import {
  verifyKey,
  copyS3Content,
  getObjectWithRetry,
  putObjectWithRetry,
} from "./utils";
import { convertSearchResults } from "./migration_utils";

const dbClient = new AWS.DynamoDB.DocumentClient();

// make sure we have defined a search bucket to store files.
const bucket = process.env.SEARCH_BUCKET;
if (!bucket) {
  throw new Error("SEARCH_BUCKET was not defined in the environment");
}

const recordsPerPage = 10;

// get all the searches from dynamodb that haven't been migrated
async function getSearchRecords(TableName, startKey) {
  const params = {
    TableName,
    Limit: recordsPerPage,
  };

  if (startKey) {
    params.ExclusiveStartKey = startKey;
  }

  try {
    const data = await dbClient.scan(params).promise();
    return data;
  } catch (err) {
    console.error(err);
    return err;
  }
}

async function backupRecord(keyPath) {
  let backupSuccess = false;
  // copy the .result file to a .result.2.0 file unless one already exists.
  const sourcePath = encodeURI(`/${bucket}/${keyPath}`);
  const newKey = keyPath.replace(/.result$/, ".result.2.0.back");
  // check for .result.2.0 file
  const backupFound = await verifyKey(bucket, newKey);
  const originalFound = await verifyKey(bucket, keyPath);
  if (backupFound) {
    backupSuccess = newKey;
  } else if (originalFound && !backupFound) {
    // if no backup already, then grab the current .result file and
    // copy it to the .result.2.0 location
    const copied = await copyS3Content(bucket, sourcePath, newKey);
    if (copied) {
      backupSuccess = newKey;
    }
  }

  // return true if a backup already exists or copying didn't fail
  return backupSuccess;
}

function getResultKeyPath(record) {
  const resultFile = record.searchMask.replace(/[^.]*$/, "result");
  const keyPath = `private/${record.identityId}/${record.searchDir}/${resultFile}`;
  return keyPath;
}

async function updateRecord(record) {
  if (record.searchMask && record.step > 3) {
    const resultKeyPath = getResultKeyPath(record);

    const backedUp = await backupRecord(resultKeyPath);
    if (backedUp) {
      // get the results file for the search from s3, using
      // the backup file path.
      console.log(`loading object from ${backedUp}`);
      const recordData = await getObjectWithRetry(bucket, backedUp);
      if (recordData.maskId) {
        // this is an old format - the new format has an inputImage instead of a maskId
        // Modify the results to match the new data format
        console.log(`Convert ${resultKeyPath} v2.0 CDS result to v3.0`);
        const converted = await convertSearchResults(
          recordData,
          record.anatomicalRegion,
          record.searchType
        );
        // Save it back to disk as the .result file.
        const complete = await putObjectWithRetry(
          bucket,
          resultKeyPath,
          converted,
          ""
        );
        if (complete) {
          console.info(`✅ converted ${record.id} to new data model`);
        } else {
          console.error(`failed to convert ${record.id} to new data model`);
        }
      } else {
        console.log(`Skip ${resultKeyPath} because it does not look like a v2.0 CDS result - maskId is missing`);
      }
    } else {
      console.warn(`⚠️  skipping ${record.id}: couldn't set a backup.`);
    }
  } else {
    console.warn(
      `⚠️  skipping ${record.id}: couldn't find a searchMask or step (${record.step}) was not > 3`
    );
  }
}

// use backup to replace .result file
async function revertRecord(record) {
  if (record.searchMask && record.step > 3) {
    const resultPath = getResultKeyPath(record);
    const backupKey = resultPath.replace(/.result$/, ".result.2.0.back");
    const checkBackup = await verifyKey(bucket, backupKey);
    if (!checkBackup) {
      console.log(
        `No backup found ${backupKey} for ${record.id}`
      );
    } else {
      const backupSource = encodeURI(`/${bucket}/${backupKey}`);

      const copied = await copyS3Content(bucket, backupSource, resultPath);
      if (copied) {
        console.log(`revert success: ${record.id}`);
      } else {
        console.error(`revert failure: ${record.id}`);
      }
    }
  } else {
    console.warn(
      `⚠️  skipping ${record.id}: couldn't find a searchMask or step (${record.step}) was not > 3`
    );
  }
}

/*
 * passing in a {"revert": true} object to the lambda function call
 * will revert any results by replacing them with the backup files.
 */
export const searchMigration = async (event) => {
  let lastEvaluatedKey = null;
  let count = 0;
  do {
    const records = await getSearchRecords(
      process.env.SEARCH_TABLE,
      lastEvaluatedKey
    );
    if (event.revert && event.revert === true) {
      await Promise.all(
        records.Items.map(async (record) => await revertRecord(record))
      );
    } else {
      await Promise.all(
        records.Items.map(async (record) => await updateRecord(record))
      );
    }
    count += records.Items.length;
    // comment next line to limit to 1 page of results
    lastEvaluatedKey = records.LastEvaluatedKey;
  } while (lastEvaluatedKey);
  console.info(`Checked ${count} records`);
};
