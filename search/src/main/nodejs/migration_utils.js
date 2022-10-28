import { queryDb } from './clientDbUtils';

function generateMipMatchPath(alignmentSpace, libraryName, fullImageName) {
  if (fullImageName) {
    if (fullImageName.includes('searchable_neurons')) {
      // this is a name of a segmented image from a searchable_neurons partition
      // we assume this is a segmentation image located in a certain partition like:
      // <as>/<library>/searchable_neurons/<partition>/<name>.tif
      // to get the displayable image we replace <partition> with "pngs" and
      // the ".tif" extension with ".png"
      const imageNameComps = fullImageName.split('/');
      const imageName = imageNameComps[imageNameComps.length - 1];
      // replace partition folder with 'pngs' folder
      imageNameComps[imageNameComps.length - 2] = 'pngs';
      // replace .tif extension with .png
      imageNameComps[imageNameComps.length - 1] = imageName.replace(/\.tiff?$/, '.png');
      return imageNameComps.join('/');
    } else {
      const parts = fullImageName.split("/");
      const filename = parts.pop();
      const imageNameComps = [
        alignmentSpace,
        libraryName,
        'searchable_neurons',
        'pngs',
        filename.replace(/\.tiff?$/, '.png')
      ];
      return imageNameComps.join("/");
    }
  } else {
    // don't know how to handle this
    return fullImageName;
  }
}

async function convertResult(result, alignmentSpace, anatomicalArea, searchType) {
  const libraryName = result.libraryName;
  const publishedName = result.publishedName;
  const publishedNamePrefix = searchType === 'lm2em'
        ? (alignmentSpace === 'JRC2018_Unisex_20x_HR' ? 'hemibrain:v1.2.1:' : 'vnc:v0.6:')
        : '';
  const targetType = searchType === 'lm2em'
        ? 'EMImage'
        : 'LMImage';
  const store = alignmentSpace === 'JRC2018_Unisex_20x_HR'
        ? 'fl:open_data:brain'
        : 'fl:pre_release:vnc';
  // if gender is not set -- this should only happen fpr EM targets
  // - set the gender to 'male' for VNC and 'female' for Brain
  const gender = result.gender
        ? result.gender
        : (anatomicalArea.toLowerCase() === 'vnc' ? 'm' : 'f');

  if (!result.imageName && !result.imageURL) {
    console.error('Result found that has neither imageName nor imageURL:', result);
  }
  const matchedImageName = result.imageName
    ? generateMipMatchPath(alignmentSpace, libraryName, result.imageName)
    : generateMipMatchPath(alignmentSpace, libraryName, result.imageURL);
  const converted = {
    image: {
      id: result.id,
      alignmentSpace,
      publishedName: `${publishedNamePrefix}${publishedName}`,
      anatomicalArea: anatomicalArea.toLowerCase() === 'vnc' ? 'VNC' : 'Brain',
      libraryName,
      gender,
      type: targetType,
      files: {
        store,
        CDM: result.imageURL,
        CDMThumbnail: result.thumbnailURL,
      },
    },
    files: {
      store,
      CDMInput: result.maskImageName || "",
      CDMMatch: matchedImageName,
    },
    mirrored: result.mirrored || false,
    normalizedScore: result.normalizedScore,
    matchingPixels: result.matchingPixels,
  };

  // figure out if this is an EM or LM result
  // based on searchType and set the appropriate attributes, eg:
  // neuronType for em results & objective for LM results.
  if (searchType === "lm2em") {
    converted.image.files.AlignedBodySWC = result.AlignedBodySWC || "";
    converted.image = {
      ...converted.image,
      neuronType: result.neuronType || "",
      neuronInstance: result.neuronInstance || "",
    };
  } else if (searchType === "em2lm") {
    converted.image.files.VisuallyLosslessStack = await getLM3DStack(alignmentSpace, result.slideCode, result.objective);
    converted.image = {
      ...converted.image,
      objective: result.objective || "",
      slideCode: result.slideCode || "",
    };
  }
  return converted;
}

async function getLM3DStack(alignmentSpace, slideCode, objective) {
  const lmPublishedStacksTable = process.env.LM_PUBLISHED_STACKS_TABLE;
  if (!lmPublishedStacksTable) {
    console.log('No table set for published LM stacks');
    return '';
  }
  const key = `${slideCode}-${objective}-${alignmentSpace}`.toLowerCase();

  const publishedImageQueryParams = {
    TableName: lmPublishedStacksTable,
    ConsistentRead: true,
    KeyConditionExpression: 'itemType = :itemType',
    ExpressionAttributeValues: {
      ':itemType': key,
    },
  };

  const publishedImageItems = await queryDb(publishedImageQueryParams);
  if (publishedImageItems && publishedImageItems.Items && publishedImageItems.Items.length > 0) {
      const publishedImage = publishedImageItems.Items[0];
      if (publishedImage.files) {
        return relativePathFromURL(publishedImage.files.VisuallyLosslessStack);
      }
  }
  return '';
}

function relativePathFromURL(aURL) {
  try {
      let protocol;
      let startPath;
      if (aURL.startsWith('https://')) {
          // the URL is: https://<awsdomain>/<bucket>/<prefix>/<fname>
          protocol = 'https://';
          startPath = 2;
      } else if (aURL.startsWith('http://')) {
          // the URL is: http://<awsdomain>/<bucket>/<prefix>/<fname>
          protocol = 'http://';
          startPath = 2;
      } else if (aURL.startsWith('s3://')) {
          // the URL is: s3://<bucket>/<prefix>/<fname>
          protocol = 's3://';
          startPath = 1;
      } else {
          // the protocol either is not set or unsupported:
          console.log(`Protocol not set or unsupported in ${aURL} - returning the value as is`);
          return aURL;
      }
      const pathComps = aURL.substring(protocol.length).split('/');
      return pathComps.slice(startPath).join('/');
  } catch (e) {
      console.error(`Erroor getting relative path for ${aURL}`, e);
      return aURL;
  }
}

export async function convertSearchResults(inputJSON, anatomicalArea, searchType) {
  const alignmentSpace = anatomicalArea.toLowerCase() === 'vnc'
    ? 'JRC2018_VNC_Unisex_40x_DS'
    : 'JRC2018_Unisex_20x_HR';
  const resultsPromises  = inputJSON.results
                            ? inputJSON.results.map(async result =>
                                await convertResult(result, alignmentSpace, anatomicalArea, searchType)
                              )
                            : [];
  const results = await Promise.all(resultsPromises);
  const output = {
    inputImage: {
      files: {
        store: '',
        CDSResults: '',
        VisuallyLosslessStack: '',
        CDMThumbnail: '',
        CDM: inputJSON.maskImageURL
          ? inputJSON.maskImageURL.split('/').slice(-2).join('/')
          : '',
      },
      alignmentSpace,
      filename: inputJSON.maskId,
      anatomicalArea: anatomicalArea.toLowerCase() === 'vnc' ? 'VNC' : 'Brain',
    },
    results,
  };
  return output;
}
