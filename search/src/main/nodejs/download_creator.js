import archiver from "archiver";
import { v1 as uuidv1 } from "uuid";
import {
  getObjectWithRetry,
  getBucketNameFromURL,
  getS3ContentAsStringWithRetry,
} from "./utils";
import { PassThrough } from "stream";
import path from "path";

import { getSearchMetadata } from "./awsappsyncutils";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

const searchBucket = process.env.SEARCH_BUCKET;
const downloadBucket = process.env.DOWNLOAD_BUCKET;
const dataBucket = process.env.DATA_BUCKET;

const s3Client = new S3Client();

async function getSearchRecord(searchId) {
  const searchMetadata = await getSearchMetadata(searchId);
  return searchMetadata;
}

async function getSearchResultsForIds(searchRecord, ids) {
  const resultFile = searchRecord.searchMask.replace(/[^.]*$/, "result");
  const resultsUrl = `private/${searchRecord.identityId}/${searchRecord.searchDir}/${resultFile}`;

  const resultObject = await getObjectWithRetry(searchBucket, resultsUrl);

  const filteredResults = resultObject.results.filter((result) =>
    ids.includes(result.image.id)
  );
  console.log(
    `found ${filteredResults.length} matching results out of ${resultObject.results.length} results`
  );
  return filteredResults;
}

async function getInteractiveSearchResults(searchId, ids) {
  // grab the searchRecord using the searchId
  const searchRecord = await getSearchRecord(searchId);

  // grab the list of results using the searchRecord
  const chosenResults = await getSearchResultsForIds(searchRecord, ids);

  return chosenResults;
}

async function getPrecomputedSearchResults(
  searchId,
  ids,
  algo = "cdm",
  version
) {
  // get results from
  // s3://janelia-neuronbridge-data-dev/{precomputedDataRootPath}/metadata/cdsresults/{searchId.json}
  const metadataDir = algo === "pppm" ? "pppmresults" : "cdsresults";
  const resultsKey = `${version}/metadata/${metadataDir}/${searchId}`;
  const resultsObj = await getObjectWithRetry(dataBucket, resultsKey);

  // filter the list of results based on the ids passed in.
  const filteredResults = resultsObj.results.filter((result) =>
    ids.includes(result.image.id)
  );
  console.log(
    `found ${filteredResults.length} matching results out of ${resultsObj.results.length} results`
  );
  return filteredResults;
}

const getReadStream = (key, sourceBucket) => {
  let streamCreated = false;

  const passThroughStream = new PassThrough();

  passThroughStream.on("newListener", (event) => {
    if (!streamCreated && event === "data") {
      console.log(`⭐  create read stream for ${sourceBucket}:${key}`);

      const s3Stream = s3Client
        .send(new GetObjectCommand({ Bucket: sourceBucket, Key: key }))
        .createReadStream();

      s3Stream
        .on("error", (err) => passThroughStream.emit("error", err))
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


function getFilePath(algo, result) {
  if (algo === "pppm") {
    return result.files.CDMBest;
  }
  return result.image.files.CDM;
}

export const downloadCreator = async (event) => {
  const downloadId = uuidv1();
  const downloadTarget = `test/${downloadId}/data.zip`;

  // Accept list of selected ids and the resultSet id/path
  const {
    ids = [],
    searchId = "",
    precomputed = false,
    algo = "cdm",
  } = event.body ? JSON.parse(event.body) : {};

  console.log(
    `looking for ids: ${ids.join(", ")} in ${precomputed ? "precomputed " : "custom "
    }search ${searchId}`
  );

  const versionFile = process.env.STAGE.match(/^prod/)
    ? "current.txt"
    : "next.txt";
  const version = await getS3ContentAsStringWithRetry(dataBucket, versionFile);
  const trimmedVersion = version.toString().replace(/\r?\n|\r/, "");

  const config = await getObjectWithRetry(
    dataBucket,
    `${trimmedVersion}/config.json`
  );

  console.log(`ℹ️  getting data for version: ${trimmedVersion}`);

  console.log(Object.keys(config.stores).join(", "));

  // Create an archive that streams directly to the download bucket.
  const archive = archiver("zip");

  // create the writer
  const uploadStream = new PassThrough();

  const writer = new Upload({
    client: s3Client,

    params: {
      Bucket: downloadBucket,
      Key: downloadTarget,
      Body: uploadStream,
      ContentType: "application/zip",
    }
  });

  archive.pipe(uploadStream);

  archive
  .on("error", (error) => {
    console.log("Archive Error");
    throw new Error(
      `${error.name} ${error.code} ${error.message} ${error.path}  ${error.stack}`
    );
  })
  .on("progress", (data) => {
    console.log("archive event: progress", data);
  });

  writer.on("close", () => {
    console.log(`✅  close write stream`);
  });
  writer.on("end", () => {
    console.log(`🛑  end write stream`);
  });
  writer.on("error", () => {
    console.log("write stream error");
    throw new Error(`Error writing to ${downloadTarget}`);
  });

  // if precomputed search, get results from different location to interactive search
  const chosenResults = precomputed
  ? await getPrecomputedSearchResults(searchId, ids, algo, trimmedVersion)
  : await getInteractiveSearchResults(searchId, ids);

  // Loop over the ids and generate streams for each one.
  chosenResults.forEach((result) => {
    console.log(`ℹ️  generating filepath from ${algo}-${result.image.id}`);
    const filePath = getFilePath(algo, result);
    console.log(filePath);
    const fileName = path.basename(filePath);
    console.log(fileName);
    // Use the information in the resultSet object to find the image path
    // and source bucket
    const storeObj = config.stores[result.image.files.store];
    const storePrefix = algo === "pppm" ? storeObj.prefixes.CDMBest : storeObj.prefixes.CDM;
    const sourceBucket = getBucketNameFromURL(storePrefix);
    // Pass the image from the source bucket into the download bucket via
    // the archiver.
    console.log(`ℹ️  appending ${sourceBucket}:${fileName} to archive`);
    archive.append(getReadStream(filePath, sourceBucket), { name: fileName });
  });

  // Once all image transfers are complete, close the archive
  console.log(`⭐  all files added to write stream`);
  archive.finalize();

  await writer.done();

  // Create a link to the newly created archive file
  // and return it as the response.
  console.log(`⭐  should be called last`);
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({ download: downloadTarget, bucket: downloadBucket }),
  };

  return returnObj;
};
