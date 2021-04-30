import archiver from "archiver";
import { v1 as uuidv1 } from "uuid";
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

const streamTo = (key, callback) => {
  var passthrough = new PassThrough();
  s3.upload(
    {
      Bucket: downloadBucket,
      Key: key,
      Body: passthrough,
      ContentType: "application/zip"
    },
    (err, data) => {
      if (err) {
        console.error("upload error", err);
      } else {
        console.log(" ❌ upload done", data);
        callback();
      }
    }
  );
  return passthrough;
};

export const downloadCreator = async event => {
  const downloadId = uuidv1();
  const downloadTarget = `test/${downloadId}/test.tar.gz`;

  // Accept list of selected ids and the resultSet id/path
  const { ids = [], searchId = "" } = event.body ? JSON.parse(event.body) : {};

  // grab the searchRecord using the searchId
  const searchRecord = await getSearchRecord(searchId);

  // grab the list of results using the searchRecord
  const chosenResults = await getSearchResultsForIds(searchRecord, ids);

  // Loop over the ids and generate streams for each one.
  await new Promise((resolve, reject) => {
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

    const writeStream = streamTo(downloadTarget, resolve);

    writeStream.on("close", () => {
      console.log(`✅  close write stream`);
      resolve();
    });
    writeStream.on("end", () => {
      console.log(`🛑  end write stream`);
      // the resolve function is no longer called here as we need it to
      // be called once the writeStream has finished, so the resolve
      // function is passed to the streamTo function as a callback to be
      // called once the stream has been closed.
    });
    writeStream.on("error", reject);

    archive.pipe(writeStream);

    chosenResults.forEach(result => {
      // Use the information in the resultSet object to find the image path
      // Pass the image from the source bucket into the download bucket via
      // the archiver.
      archive.append(getStream(result.imageName), { name: result.imageName });
    });

    // Once all image transfers are complete, close the archive
    console.log(`⭐  finalizing write stream`);
    archive.finalize();
  }).catch(error => {
    throw new Error(`${error.code} ${error.message} ${error.data}`);
  });

  // Create a link to the newly created archive file and return it
  // as the response.
  console.log(`⭐  should be called last`);
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({ download: downloadTarget, bucket: downloadBucket })
  };

  return returnObj;
};
