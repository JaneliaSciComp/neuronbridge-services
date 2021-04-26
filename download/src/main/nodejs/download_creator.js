import archiver from "archiver";
import AWS from "aws-sdk";
import { getObjectWithRetry, putObject } from "./utils";
import { PassThrough } from "stream";

import { getSearchMetadata } from "./awsappsyncutils";

const searchBucket = process.env.SEARCH_BUCKET;
const libraryBucket = process.env.LIBRARY_BUCKET;
const downloadBucket = process.env.DOWNLOAD_BUCKET;

const s3 = new AWS.S3();

async function getSearchRecord(searchId) {
  const searchMetadata = await getSearchMetadata(searchId);
  return searchMetadata;
}

async function getSearchResultsForIds(searchRecord, ids) {
  const resultFile = searchRecord.searchMask.replace(/[^.]*$/, "result");
  const resultsUrl = `private/${searchRecord.identityId}/${searchRecord.searchDir}/${resultFile}`;

  const resultObject = await getObjectWithRetry(searchBucket, resultsUrl);

  return resultObject.results.filter(result => ids.includes(result.id));
}

const getStream = key => {
  let streamCreated = false;

  const passThroughStream = new PassThrough();

  passThroughStream.on("newListener", event => {
    if (!streamCreated && event === "data") {
      console.log(`⭐  create stream for key ${key}`);

      const s3Stream = s3
        .getObject({ Bucket: searchBucket, Key: key })
        .createReadStream();

      s3Stream
        .on("error", err => passThroughStream.emit("error", err))
        .on("finish", () => console.log(`✅  finish stream for key ${key}`))
        .on("close", () => console.log(`❌  stream close\n`))
        .pipe(passThroughStream);

      streamCreated = true;
    }
  });
  return passThroughStream;
};

export const downloadCreator = async event => {
  // Accept list of selected ids and the resultSet id/path
  const { ids = [], searchId = "" } = event.body ? JSON.parse(event.body) : {};

  // grab the searchRecord using the searchId
  const searchRecord = await getSearchRecord(searchId);

  // grab the list of results using the searchRecord
  const chosenResults = await getSearchResultsForIds(searchRecord, ids);

  // Create an archive that streams directly to the download bucket.
  const archive = archiver("tar", {
    gzip: true,
    gzipOptions: {
      level: 1
    }
  });
  archive.on("error", error => {
    throw new Error(
      `${error.name} ${error.code} ${error.message} ${error.path}  ${error.stack}`
    );
  });

  // create the upload stream to write the archive
  const streamPassThrough = new PassThrough();
  const writeStreamParameters = {
    ACL: "public-read",
    Body: streamPassThrough,
    ContentType: "application/zip",
    Bucket: downloadBucket,
    Key: "test/test.tar.gz"
  };

  const testParameters = {
    ACL: "public-read",
    Body: "This is a test",
    ContentType: "text/plain",
    Bucket: downloadBucket,
    Key: "test/test.text"
  };

  // this works now that the write permissions are in place.
  const location = await putObject(downloadBucket, "test/test.json", {test: "this is a test"});

  console.log({ location });

  s3.upload(testParameters, (err, data) => {
    if (err) {
      console.error("upload error", err);
    } else {
      console.log("upload done", data);
    }
  });

  const writeStream = s3.upload(writeStreamParameters, (err, data) => {
    if (err) {
      console.error("upload error", err);
    } else {
      console.log("upload done", data);
    }
  });

  // Loop over the ids and generate streams for each one.
  const added = [];

  await new Promise((resolve, reject) => {
    writeStream.on("close", resolve());
    writeStream.on("end", resolve());
    writeStream.on("error", reject());

    archive.pipe(streamPassThrough);

    chosenResults.forEach(result => {
      // Use the information in the resultSet object to find the image path
      // Pass the image from the source bucket into the download bucket via
      // the archiver.
      archive.append(getStream(result.imageName), { name: result.imageName });
      added.push(`s3://${libraryBucket}/${result.imageName}`);
    });

    // Once all image transfers are complete, close the archive
    archive.finalize();
  }).catch(error => {
    throw new Error(`${error.code} ${error.message} ${error.data}`);
  });

  // Create a link to the newly created archive file and return it
  // as the response.
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({ ids, images: added })
  };

  return returnObj;
};
