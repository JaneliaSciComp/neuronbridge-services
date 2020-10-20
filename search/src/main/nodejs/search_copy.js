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
  const thumbnailName = 'upload_thumbnail.png';
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
    upload,
    searchMask
  } = searchData;
  // generate a new id for the search directory
  const newSearchDir = uuidv1();
  const newSearchInputFolder = `private/${identityId}/${newSearchDir}`;

  // copy uploaded image
  await copyS3Content(
    searchBucket,
    `/${searchBucket}/${searchInputFolder}/${upload}`,
    `${newSearchInputFolder}/${upload}`
  );
  // copy thumbnail image
  await copyS3Content(
    searchBucket,
    `/${searchBucket}/${searchInputFolder}/upload_thumbnail.png`,
    `${newSearchInputFolder}/upload_thumbnail.png`
  );
  // copy display image
  await copyS3Content(
    searchBucket,
    `/${searchBucket}/${searchInputFolder}/${searchMask}`,
    `${newSearchInputFolder}/${searchMask}`
  );

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
      await copyS3Content(searchBucket, `${searchBucket}/${channel}`, newChannelPath);
    })
  );



  // create new data object to store in dynamoDB
  const newSearchData = {
    step: ALIGNMENT_JOB_COMPLETED,
    searchType: searchData.searchType,
    owner: searchData.owner,
    identityId: searchData.identityId,
    searchDir: newSearchDir,
    upload: searchData.upload,
    simulateMIPGeneration: false,
    uploadThumbnail: 'upload_thumbnail.png'
  };
  // save new data object- in dynamoDB
  const newSearchMeta = await createSearchMetadata(newSearchData);
  return { searchData, newSearchData, newSearchMeta, channelsList };
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
