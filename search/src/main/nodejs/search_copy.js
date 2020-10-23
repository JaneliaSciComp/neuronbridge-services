"use strict";

const Jimp = require("jimp");
const { v1: uuidv1 } = require("uuid");
const {
  getS3ContentWithRetry,
  copyS3Content,
  putS3Content,
  getAllKeys
} = require("./utils");
const { getSearchKey, getSearchMaskId } = require("./searchutils");
const {
  updateSearchMetadata,
  createSearchMetadata,
  getSearchMetadata,
  ALIGNMENT_JOB_COMPLETED
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
  // png, jpeg, bmp, tiff or gif images that are already aligned. The 3D stacks
  // need to go through the aligner which will output the channels for masking

  const fullSearchInputImage = `${searchInputFolder}/${upload}`;
  // grab the image data
  const imageContent = await getS3ContentWithRetry(
    searchBucket,
    fullSearchInputImage,
    s3Retries
  );

  let sourceImage = fullSearchInputImage;
  let channelName = upload.replace(/\.([^.]*)$/, "_1.$1");

  const searchMetaData = {
    id,
    step: ALIGNMENT_JOB_COMPLETED
  };

  const pngMime = "image/png";

  // if not a png, transform to png
  if (/\.(tiff?|gif|jpe?g|bmp)$/.test(upload)) {
    const pngExt = ".png";
    const image = await Jimp.read(imageContent);
    const imageBuffer = await image.getBufferAsync(pngMime);
    const pngImageName = getSearchKey(fullSearchInputImage, pngExt);
    sourceImage = pngImageName;
    channelName = upload.replace(/\.([^.]*)$/, "_1.png");
    await putS3Content(searchBucket, pngImageName, pngMime, imageBuffer);
    searchMetaData.displayableMask = getSearchMaskId(pngImageName, pngExt);
  }
  // create new file in generatedMIPS directory as channel_1.png
  const channelPath = `private/${identityId}/${searchDir}/generatedMIPS/${channelName}`;
  await copyS3Content(
    searchBucket,
    `/${searchBucket}/${sourceImage}`,
    channelPath
  );

  // create a thumbnail of the uploaded image
  const thumbnailName = "upload_thumbnail.png";
  const original = await Jimp.read(imageContent);
  const thumbnail = original.scaleToFit(150, 70);
  const thumbnailBuffer = await thumbnail.getBufferAsync(pngMime);
  const thumbnailPath = `private/${identityId}/${searchDir}/${thumbnailName}`;
  await putS3Content(searchBucket, thumbnailPath, pngMime, thumbnailBuffer);
  searchMetaData.uploadThumbnail = thumbnailName;

  await updateSearchMetadata(searchMetaData);
  return { id };
}

async function copyAlignment(searchData) {
  const {
    identityId,
    searchDir,
    searchInputFolder,
    uploadThumbnail,
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

  // create new data object to store in dynamoDB
  const newSearchData = {
    step: ALIGNMENT_JOB_COMPLETED,
    owner: searchData.owner,
    identityId: searchData.identityId,
    searchDir: newSearchDir,
    upload: searchData.upload,
    simulateMIPGeneration: false,
    uploadThumbnail: searchData.uploadThumbnail
  };
  // save new data object- in dynamoDB
  const newSearchMeta = await createSearchMetadata(newSearchData);
  return { newSearchData, newSearchMeta };
}

/*
 *  Given an s3 image url and a users identity, this function will
 *  copy that image into a new search record and set it up so that
 *  the user can create a new mask and run a search.
 */
async function createNewSearchFromImage(image, event, identityId) {
  // generate a new id for the search directory
  const newSearchId = uuidv1();
  // create new search directory
  const newSearchFolder = `private/${identityId}/${newSearchId}`;
  // convert image.imageURL to bucket location
  const [, originalBucket, originalPath, originalImage] = image.imageURL.match(
    /^.*s3.amazonaws.com\/([^/]*)(.*?)([^/]*)$/
  );
  // copy image to upload
  await copyS3Content(
    searchBucket,
    `/${originalBucket}/${originalPath}/${originalImage}`,
    `${newSearchFolder}/${originalImage}`
  );

  // convert image.thumbnailURL to new bucket location
  const [, thumbnailBucket, thumbnailPath, thumbnailUpload] = image.thumbnailURL.match(
    /^.*s3.amazonaws.com\/([^/]*)(.*?)([^/]*)$/
  );
  // copy image thumbnail to new bucket
  await copyS3Content(
    searchBucket,
    `/${thumbnailBucket}/${thumbnailPath}/${thumbnailUpload}`,
    `${newSearchFolder}/upload_thumbnail.png`
  );
  //
  // generate mip channel for this image
  let channelName = originalImage.replace(/\.([^.]*)$/, "_1.$1");
  const channelPath = `private/${identityId}/${newSearchId}/generatedMIPS/${channelName}`;
  await copyS3Content(
    searchBucket,
    `/${originalBucket}/${originalPath}/${originalImage}`,
    channelPath
  );

  // create new data object to store in dynamoDB
  const ownerId = event.requestContext.authorizer.jwt.claims.sub;
  // set step to mask selection
  const newSearchData = {
    step: ALIGNMENT_JOB_COMPLETED,
    owner: ownerId,
    identityId: identityId,
    searchDir: newSearchFolder,
    upload: originalImage,
    simulateMIPGeneration: false,
    uploadThumbnail: thumbnailUpload
  };
  // save new data object- in dynamoDB
  const newSearchMeta = await createSearchMetadata(newSearchData);

  return { event, image, identityId, newSearchMeta };
}

exports.searchCopy = async (event) => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  try {
    // get the search id from the post body
    const { searchId, action, image, identityId } = JSON.parse(event.body);
    if (action === "create_default_channel") {
      // fetch search information from dynamoDB
      const searchData = await getSearchRecord(searchId);
      returnBody = await createDefaultChannel(searchData);
    } else if (action === "alignment_copy") {
      // fetch search information from dynamoDB
      const searchData = await getSearchRecord(searchId);
      returnBody = await copyAlignment(searchData);
    } else if (action === "new_search_from_image") {
      returnBody = await createNewSearchFromImage(image, event, identityId);
    }
  } catch (error) {
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }

  returnObj.body = JSON.stringify(returnBody);

  return returnObj;
};
