import { v1 as uuidv1 } from "uuid";
import { copyS3Content, getAllKeys } from "./utils";
import {
  createSearchMetadata,
  updateSearchMetadata,
  getSearchMetadata,
  ALIGNMENT_JOB_COMPLETED
} from "./awsappsyncutils";

const searchBucket = process.env.SEARCH_BUCKET;

async function getSearchRecord(searchId) {
  const searchMetadata = await getSearchMetadata(searchId);
  return searchMetadata;
}

async function copyAlignment(searchData) {
  const {
    identityId,
    searchDir,
    searchInputFolder,
    uploadThumbnail,
    alignmentMovie,
  } = searchData;
  // generate a new id for the search directory
  const newSearchDir = uuidv1();
  const newSearchInputFolder = `private/${identityId}/${newSearchDir}`;

  // only copy the upload thumbnail and the MIP channels as the original upload image
  // is a) no longer needed and b) possibly missing. These are all the files that we
  // need to start a new search.

  if (uploadThumbnail) {
    // copy thumbnail image
    await copyS3Content(
      searchBucket,
      `/${searchBucket}/${searchInputFolder}/${uploadThumbnail}`,
      `${newSearchInputFolder}/${uploadThumbnail}`
    );
  }
  if (alignmentMovie && alignmentMovie !== 'None') {
    // copy alignment movie
    await copyS3Content(
      searchBucket,
      `/${searchBucket}/${searchInputFolder}/${alignmentMovie}`,
      `${newSearchInputFolder}/${alignmentMovie}`
    );
  }
  // copy MIP channels
  const channelsPath = `private/${identityId}/${searchDir}/generatedMIPS`;
  const channelsList = await getAllKeys({
    Bucket: searchBucket,
    Prefix: channelsPath
  });

  const newChannelsPath = `${newSearchInputFolder}/generatedMIPS`;
  await Promise.all(
    channelsList.map(async channel => {
      const newChannel = channel.split("/").pop();
      const newChannelPath = `${newChannelsPath}/${newChannel}`;
      await copyS3Content(
        searchBucket,
        `${searchBucket}/${channel}`,
        newChannelPath
      );
    })
  );

  // create new search metadata object.
  const newSearchData = {
    step: ALIGNMENT_JOB_COMPLETED,
    owner: searchData.owner,
    identityId: searchData.identityId,
    searchDir: newSearchDir,
    upload: searchData.upload,
    anatomicalRegion: searchData.anatomicalRegion || 'brain',
    simulateMIPGeneration: false,
    uploadThumbnail: searchData.uploadThumbnail,
    dataThreshold: searchData.dataThreshold,
    searchType: searchData.searchType,
    maskThreshold: searchData.maskThreshold,
    mirrorMask: searchData.mirrorMask,
    xyShift: searchData.xyShift,
    pixColorFluctuation: searchData.pixColorFluctuation,
  };
  // save new data object in dynamoDB
  const newSearchMeta = await createSearchMetadata(newSearchData);

  //add alignment info if present
  const newSearchMetaWithAlignment = await updateSearchMetadata({
    id: newSearchMeta.searchId,
    alignStarted: searchData.alignStarted,
    alignFinished: searchData.alignFinished,
    alignmentSize: searchData.alignmentSize,
    alignmentErrorMessage: searchData.alignmentErrorMessage,
    alignmentMovie: searchData.alignmentMovie,
    alignmentScore: searchData.alignmentScore,
    computedMIPs: searchData.computedMIPs,
  });

  return { newSearchData, newSearchMeta: newSearchMetaWithAlignment };
}

export const searchCopyAlignment = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  try {
    // get the search id from the post body
    const { searchId } = JSON.parse(event.body);
    const searchData = await getSearchRecord(searchId);
    returnBody = await copyAlignment(searchData);
  } catch (error) {
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }

  returnObj.body = JSON.stringify(returnBody);

  return returnObj;
};
