import archiver from 'archiver';
import { v1 as uuidv1 } from 'uuid';
import {
  getObjectWithRetry,
  getBucketNameFromURL,
  getS3ContentAsStringWithRetry,
  getS3ContentAsByteBufferWithRetry,
} from './utils';
import { PassThrough } from 'stream';
import path from 'path';

import { getSearchMetadata } from './awsappsyncutils';
import { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';

const searchBucket = process.env.SEARCH_BUCKET;
const downloadBucket = process.env.DOWNLOAD_BUCKET;
const dataBucket = process.env.DATA_BUCKET;
const archiverBufferSizeInMB = process.env.ARCHIVER_BUFFER_MB || '128';

const s3Client = new S3Client();

async function getSearchRecord(searchId) {
  const searchMetadata = await getSearchMetadata(searchId);
  return searchMetadata;
}

async function getSearchResultsForIds(searchRecord, ids) {
  const resultFile = searchRecord.searchMask.replace(/[^.]*$/, 'result');
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

async function getPrecomputedSearchResults(searchId, ids, algo = 'cdm', version) {
  // get results from
  // s3://janelia-neuronbridge-data-dev/{precomputedDataRootPath}/metadata/cdsresults/{searchId.json}
  const metadataDir = algo === 'pppm' ? 'pppmresults' : 'cdsresults';
  const resultsKey = `${version}/metadata/${metadataDir}/${searchId}`;
  const resultsObj = await getObjectWithRetry(dataBucket, resultsKey);

  // filter the list of results based on the ids passed in.
  const filteredResults = resultsObj.results.filter((result) =>
    ids.includes(result.image.id)
  );
  console.log(
    `found ${filteredResults.length} matching results `,
    `out of ${resultsObj.results.length} results`,
  );
  return filteredResults;
}

export const downloadCreator = async (event) => {
  const downloadId = uuidv1();
  const downloadTarget = `test/${downloadId}/data.zip`;

  // Accept list of selected ids and the resultSet id/path
  const {
    ids = [],
    searchId = '',
    precomputed = false,
    algo = 'cdm',
  } = event.body ? JSON.parse(event.body) : {};

  console.log(
    `download: ${downloadId}`,
    `looking for ids: ${ids.join(', ')} in `,
    `${precomputed ? 'precomputed ' : 'custom '} search ${searchId}`,
  );

  const versionFile = process.env.STAGE.match(/^prod/)
    ? 'current.txt'
    : 'next.txt';
  const version = await getS3ContentAsStringWithRetry(dataBucket, versionFile);
  const trimmedVersion = version.toString().replace(/\r?\n|\r/, '');

  const config = await getObjectWithRetry(dataBucket, `${trimmedVersion}/config.json`);

  console.log(`ℹ️  getting data for version: ${trimmedVersion}`);

  console.log(Object.keys(config.stores).join(', '));

  // if precomputed search, get results from different location to interactive search
  const chosenResults = precomputed
    ? await getPrecomputedSearchResults(searchId, ids, algo, trimmedVersion)
    : await getInteractiveSearchResults(searchId, ids);

  console.log(`Prepare to upload chosen results to ${downloadBucket}:${downloadTarget}`);

  try {
    // Create an archive that streams directly to the download bucket.
    const archive = archiver('zip', {
      zlib: { level: 0 },
    });
    const writer = writeContentTo('application/zip', archive, downloadBucket, downloadTarget);
    // Loop over the ids and generate streams for each one.
    for (let i = 0; i < chosenResults.length; i++) {
      const result = chosenResults[i];
      console.log(`ℹ️ process entry ${i} from ${algo}-${result.image.id}`);
      const filePath = algo === 'pppm' ? result.files.CDMBest : result.image.files.CDM;
      const fileName = path.basename(filePath);
      console.log(`${filePath} -> ${fileName}`);
      // Use the information in the resultSet object to find the image path
      // and source bucket
      const storeObj = config.stores[result.image.files.store];
      const storePrefix = algo === 'pppm' ? storeObj.prefixes.CDMBest : storeObj.prefixes.CDM;
      const sourceBucket = getBucketNameFromURL(storePrefix);
      // Pass the image from the source bucket into the download bucket via the archiver.
      const archiveEntry = await getS3ContentAsByteBufferWithRetry(sourceBucket, filePath);
      archive.append(archiveEntry, { name: fileName });
      console.log(`✅ entry ${i}: ${sourceBucket}:${fileName} - added to archive`);
    }
    archive.finalize();

    await writer.done();

    // Create a link to the newly created archive file
    // and return it as the response.
    console.log(`⭐  return download link`);
    return {
      isBase64Encoded: false,
      statusCode: 200,
      body: JSON.stringify({ download: downloadTarget, bucket: downloadBucket }),
    };
  } catch(err) {
    console.error('❌ Upload error', err);
    throw err;
  }
};

const writeContentTo = (ContentType, ContentStream, Bucket, Key) => {
  console.log(`⭐  write content to ${Bucket}:${Key} for content-type: ${ContentType}`);

  const writeStream = new PassThrough({
    highWaterMark: archiverBufferSizeInMB * 1024 * 1024,
  });
  ContentStream.pipe(writeStream);
  // create the writer
  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket,
      Key,
      Body: writeStream,
      ContentType,
    },
  });

  return upload;
};
