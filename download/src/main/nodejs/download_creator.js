import archiver from "archiver";
import AWS from "aws-sdk";
import { getObjectWithRetry } from "./utils";
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
      console.log(`⭐  create read stream for ${libraryBucket}:${key}`);

      const s3Stream = s3
        .getObject({ Bucket: libraryBucket, Key: key })
        .createReadStream();

      s3Stream
        .on("error", err => passThroughStream.emit("error", err))
        .on("finish", () =>
          console.log(`✅  finish read stream for key ${key}`)
        )
        .on("close", () => console.log(`❌  read stream closed\n`))
        .pipe(passThroughStream);

      streamCreated = true;
    }
  });
  return passThroughStream;
};

const streamTo = key => {
  var passthrough = new PassThrough();
  s3.upload(
    {
      Bucket: downloadBucket,
      Key: key,
      Body: passthrough,
      ContentType: "application/zip"
    },
    (err, data) => {
      if (err) throw err;
    }
  );
  return passthrough;
};

export const downloadCreator = async event => {
  // test writing to bucket works.
  // await putObject(downloadBucket, "test/test.json", { test: "this is a test" });
  // test streaming uploads to the download bucket
  // await testUpload();

  // Accept list of selected ids and the resultSet id/path
  const { ids = [], searchId = "" } = event.body ? JSON.parse(event.body) : {};

  // grab the searchRecord using the searchId
  const searchRecord = await getSearchRecord(searchId);

  // grab the list of results using the searchRecord
  const chosenResults = await getSearchResultsForIds(searchRecord, ids);

  // Loop over the ids and generate streams for each one.
  const added = [];

  await new Promise(async (resolve, reject) => {
    // Create an archive that streams directly to the download bucket.
    const archive = archiver("tar", {
      gzip: true,
      gzipOptions: {
        level: 1
      }
    });
    archive
      .on("error", error => {
        throw new Error(
          `${error.name} ${error.code} ${error.message} ${error.path}  ${error.stack}`
        );
      })
      .on("progress", data => {
        console.log("archive event: progress", data);
      });

    const writeStream = streamTo(`test/${Math.random()}/test.tar.gz`);

    writeStream.on("close", () => {
      console.log(`✅  close write stream`);
      resolve();
    });
    writeStream.on("end", () => {
      console.log(`✅  end write stream`);
      // Can't call this resolve as it seems to stop the zip from being closed.
      // If the resolve is enabled, the zip file doesn't get written out to the
      // s3 bucket, until after the lambda is called a second time. The result
      // is 0 files on first all and 2 files on second call.
      // resolve();
    });
    writeStream.on("error", reject);

    archive.pipe(writeStream);

    chosenResults.forEach(result => {
      // Use the information in the resultSet object to find the image path
      // Pass the image from the source bucket into the download bucket via
      // the archiver.
      archive.append(getStream(result.imageName), { name: result.imageName });
      added.push(`s3://${libraryBucket}/${result.imageName}`);
    });

    // Once all image transfers are complete, close the archive
    console.log(`⭐  finalizing write stream`);
    archive.finalize();
  })
    .then()
    .catch(error => {
      throw new Error(`${error.code} ${error.message} ${error.data}`);
    });

  // Create a link to the newly created archive file and return it
  // as the response.
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({ ids, images: added })
  };

  console.log(`⭐  should be called last`);
  return returnObj;
};
