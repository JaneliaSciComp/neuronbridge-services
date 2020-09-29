"use strict";

const Jimp = require("jimp");
const { v1: uuidv1 } = require("uuid");
const {
  getS3ContentWithRetry,
  copyS3Content,
  putS3Content
} = require("./utils");
const { getSearchKey, getSearchMaskId } = require("./searchutils");
const {
  updateSearchMetadata,
  getSearchMetadata
} = require("./awsappsyncutils");

const searchBucket = process.env.SEARCH_BUCKET;
const s3Retries = process.env.S3_RETRIES || 3;

async function getSearchRecord(searchId) {
  const searchMetadata = await getSearchMetadata(searchId);
  return searchMetadata;
}

async function createDefaultChannel(searchData) {
  const { id, upload, searchInputFolder, identityId, searchDir } = searchData;
  // TODO: add a check for the image extension here. We should only be copying
  // png, jpeg, tiff or gif? images that are already aligned. The 3D stacks
  // need to go through the aligner which will output the channels for masking

  const fullSearchInputImage = `${searchInputFolder}/${upload}`;
  // grab the image data
  const imageContent = await getS3ContentWithRetry(
    searchBucket,
    fullSearchInputImage,
    s3Retries
  );

  let sourceImage = fullSearchInputImage;
  let channelName = upload.replace(/\.([^.]*)$/,'_1.$1');

  const searchMetaData = {
    id,
    step: 2
  };

  // if tiff, transform to png
  if (/\.tiff?$/.test(upload)) {
    const pngMime = "image/png";
    const pngExt = ".png";
    const image = await Jimp.read(imageContent);
    const imageBuffer = await image.getBufferAsync(pngMime);
    const pngImageName = getSearchKey(fullSearchInputImage, pngExt);
    sourceImage = pngImageName;
    channelName = upload.replace(/\.([^.]*)$/,'_1.png');
    await putS3Content(searchBucket, pngImageName, pngMime, imageBuffer);
    searchMetaData.displayableMask = getSearchMaskId(pngImageName, pngExt);
  }
  // create new file in generatedMIPS directory as channel_1.png
  const channelPath = `private/${identityId}/${searchDir}/generatedMIPS/${channelName}`;
  await copyS3Content(searchBucket, `/${searchBucket}/${sourceImage}`, channelPath);
  await updateSearchMetadata(searchMetaData);
  return { id };
}

async function copyAlignment(searchData) {
  // generate a new id for the search
  const newSearchId = uuidv1();
  console.log({ searchData, newSearchId });
  // copy masks and uploaded image to new location in bucket
  // set search step to 2 -> alignment completed
  // set mask selection to null
  // save new search in dynamoDB
  throw Error("Not implimented");
  // return { searchData, searchId: newSearchId };
}

exports.searchCopy = async event => {
  console.log(event);
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  try {
    // get the search id from the post body
    const { searchId, action } = JSON.parse(event.body);

    // fetch search information from dynamoDB
    const searchData = await getSearchRecord(searchId);

    if (action === "create_default_channel") {
      returnBody = await createDefaultChannel(searchData);
    } else if (action === "alignment_copy") {
      returnBody = await copyAlignment(searchData);
    }
  } catch (error) {
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }

  returnObj.body = JSON.stringify(returnBody);

  return returnObj;
};
