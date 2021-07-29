import { v1 as uuidv1 } from "uuid";
import { copyS3Content } from "./utils";
import {
  createSearchMetadata,
  ALIGNMENT_JOB_COMPLETED
} from "./awsappsyncutils";

const searchBucket = process.env.SEARCH_BUCKET;

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
  console.log(`copying image from /${originalBucket}${originalPath}${originalImage} to ${newSearchFolder}/${originalImage}`);
  await copyS3Content(
    searchBucket,
    `/${originalBucket}${originalPath}${originalImage}`,
    `${newSearchFolder}/${originalImage}`
  );

  // convert image.thumbnailURL to new bucket location
  const [
    ,
    thumbnailBucket,
    thumbnailPath,
    thumbnailUpload,
    extension
  ] = image.thumbnailURL.match(
    /^.*s3.amazonaws.com\/([^/]*)(.*?)([^/]*?)([^.]*)$/
  );

  const newThumbnailName = `upload_thumbnail.${extension}`;

  // copy image thumbnail to new bucket
  console.log(`copying thumbnail from /${thumbnailBucket}${thumbnailPath}${thumbnailUpload}${extension} to ${newSearchFolder}/${newThumbnailName}`);
  await copyS3Content(
    searchBucket,
    `/${thumbnailBucket}${thumbnailPath}${thumbnailUpload}${extension}`,
    `${newSearchFolder}/${newThumbnailName}`
  );
  //
  // generate mip channel for this image
  let channelName = originalImage.replace(/\.([^.]*)$/, "_1.$1");
  const channelPath = `private/${identityId}/${newSearchId}/generatedMIPS/${channelName}`;
  await copyS3Content(
    searchBucket,
    `/${originalBucket}${originalPath}${originalImage}`,
    channelPath
  );

  // create new data object to store in dynamoDB
  //
  // DynamoDB uses the value in the owner field to determine access to the
  // search records. When creating the new search after copying the image to
  // be masked, the lambda function was using the users 'sub' to set the
  // owner. For AWS logins, this is fine, but when logged in with a google
  // account, the users sub is not the same as the username. Switching to
  // username seems to get the code working for both types of logins.
  const ownerId = event.requestContext.authorizer.jwt.claims.username;
  // set step to mask selection
  const newSearchData = {
    step: ALIGNMENT_JOB_COMPLETED,
    owner: ownerId,
    identityId: identityId,
    searchDir: newSearchId,
    upload: originalImage,
    simulateMIPGeneration: false,
    uploadThumbnail: newThumbnailName
  };
  // save new data object- in dynamoDB
  const newSearchMeta = await createSearchMetadata(newSearchData);
  console.log(`Created new search in dynamoDB: ${newSearchMeta.id}`);

  return { newSearchMeta };
}

export const searchNewFromImage = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  try {
    // get the search id from the post body
    const { image, identityId } = JSON.parse(event.body);
    returnBody = await createNewSearchFromImage(image, event, identityId);
  } catch (error) {
    console.log(error);
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }

  returnObj.body = JSON.stringify(returnBody);

  return returnObj;
};
