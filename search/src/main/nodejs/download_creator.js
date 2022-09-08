import archiver from "archiver";
import { v1 as uuidv1 } from "uuid";
import AWS from "aws-sdk";
import { getObjectWithRetry, getS3ContentWithRetry } from "./utils";
import { PassThrough } from "stream";
import path from "path";

import { getSearchMetadata } from "./awsappsyncutils";

const searchBucket = process.env.SEARCH_BUCKET;
const libraryBucket = process.env.LIBRARY_BUCKET;
const downloadBucket = process.env.DOWNLOAD_BUCKET;
const dataBucket = process.env.DATA_BUCKET;
const pppBucket = process.env.PPP_BUCKET;

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

async function getInteractiveSearchResults(searchId, ids) {
  // grab the searchRecord using the searchId
  const searchRecord = await getSearchRecord(searchId);

  // grab the list of results using the searchRecord
  const chosenResults = await getSearchResultsForIds(searchRecord, ids);

  return [chosenResults];
}

async function getPrecomputedSearchResults(searchId, ids, algo="cdm") {
  // get precomputedDataRootPath from s3://janelia-neuronbridge-data-dev/paths.json
  const version = await getS3ContentWithRetry(dataBucket, "current.txt");
  const trimmedVersion = version.toString().replace(/\r?\n|\r/,'');
  const metadataDir = (algo === "ppp") ? 'pppresults' : 'cdsresults';

  // get results from
  // s3://janelia-neuronbridge-data-dev/{precomputedDataRootPath}/metadata/cdsresults/{searchId}.json
  const resultsKey = `${trimmedVersion}/metadata/${metadataDir}/${searchId}.json`;
  const resultsObj = await getObjectWithRetry(dataBucket, resultsKey);

  // filter the list of results based on the ids passed in.
  return [resultsObj.results.filter(result => ids.includes(result.id)), resultsObj.maskLibraryName];
}

const getReadStream = (key, algo) => {
  let streamCreated = false;

  const passThroughStream = new PassThrough();
  const Bucket = algo === 'ppp' ? pppBucket : libraryBucket;

  passThroughStream.on("newListener", event => {
    if (!streamCreated && event === "data") {
      console.log(`⭐  create read stream for ${Bucket}:${key}`);

      const s3Stream = s3
        .getObject({ Bucket, Key: key })
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
  const uploadParams = {
    Bucket: downloadBucket,
    Key: key,
    Body: passthrough,
    ContentType: "application/zip"
  };
  s3.upload(uploadParams,
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

function getFilePath(algo, result, library) {
  if (algo === "ppp") {
    return `${result.alignmentSpace}/${library}/${result.files.ColorDepthMip}`;
  }
  return result.imageName ? result.imageName : result.imageURL;
}

export const downloadCreator = async (event) => {
  const downloadId = uuidv1();
  const downloadTarget = `test/${downloadId}/data.zip`;

  // Accept list of selected ids and the resultSet id/path
  const { ids = [], searchId = "", precomputed = false, algo="cdm" } = event.body
    ? JSON.parse(event.body)
    : {};

  // if precomputed search, get results from different location to interactive search
  const [chosenResults, library] = precomputed
    ? await getPrecomputedSearchResults(searchId, ids, algo)
    : await getInteractiveSearchResults(searchId, ids);

  // Loop over the ids and generate streams for each one.
  await new Promise((resolve, reject) => {
    const writeStream = streamTo(downloadTarget, resolve);

    writeStream.on("close", () => {
      console.log(`✅  close write stream`);
    });
    writeStream.on("end", () => {
      console.log(`🛑  end write stream`);
      // the resolve function is no longer called here as we need it to
      // be called once the writeStream has finished, so the resolve
      // function is passed to the streamTo function as a callback to be
      // called once the stream has been closed.
    });
    writeStream.on("error", () => {
      console.log("write stream error");
      reject();
    });

    // Create an archive that streams directly to the download bucket.
    const archive = archiver("zip");
    archive
      .on("error", error => {
        console.log("Archive Error");
        throw new Error(
          `${error.name} ${error.code} ${error.message} ${error.path}  ${error.stack}`
        );
      })
      .on("progress", data => {
        console.log("archive event: progress", data);
      });

    archive.pipe(writeStream);

    chosenResults.forEach(result => {
      const filePath = getFilePath(algo, result, library);
      const fileName = path.basename(filePath);
      // Use the information in the resultSet object to find the image path
      // Pass the image from the source bucket into the download bucket via
      // the archiver.
      console.log(`ℹ️  appending ${fileName} to archive`);
      archive.append(getReadStream(filePath, algo), { name: fileName });
    });

    // Once all image transfers are complete, close the archive
    console.log(`⭐  all files added to write stream`);
    archive.finalize();
  }).catch(error => {
    console.log("Promise Error");
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
