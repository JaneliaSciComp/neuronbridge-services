import Jimp from "jimp";
import {
  getS3ContentWithRetry,
  copyS3Content,
  putS3Content,
} from "./utils";
import { getSearchKey, getSearchMaskId } from "./searchutils";
import {
  updateSearchMetadata,
  getSearchMetadata,
  ALIGNMENT_JOB_COMPLETED
} from "./awsappsyncutils";

const searchBucket = process.env.SEARCH_BUCKET;
const s3Retries = process.env.S3_RETRIES || 3;

async function getSearchRecord(searchId) {
  const searchMetadata = await getSearchMetadata(searchId);
  return searchMetadata;
}

async function createDefaultChannel(searchData) {
  const { id, upload, searchInputFolder, identityId, searchDir, anatomicalRegion } = searchData;
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
  const thumbnail = anatomicalRegion === "vnc" ? original.scaleToFit(70, 150) : original.scaleToFit(150, 70);
  const thumbnailBuffer = await thumbnail.getBufferAsync(pngMime);
  const thumbnailPath = `private/${identityId}/${searchDir}/${thumbnailName}`;
  await putS3Content(searchBucket, thumbnailPath, pngMime, thumbnailBuffer);
  searchMetaData.uploadThumbnail = thumbnailName;

  await updateSearchMetadata(searchMetaData);
  return { id };
}

export const searchCreateDefaultChannel = async (event) => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  try {
    const { searchId } = JSON.parse(event.body);
      const searchData = await getSearchRecord(searchId);
      returnBody = await createDefaultChannel(searchData);
  } catch (error) {
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }

  returnObj.body = JSON.stringify(returnBody);

  return returnObj;
};
