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

function convertResult(result, alignmentSpace, anatomicalArea, searchType) {
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
        AlignedBodySWC: result.AlignedBodySWC || "",
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
    converted.image = {
      ...converted.image,
      neuronType: result.neuronType || "",
      neuronInstance: result.neuronInstance || "",
    };
  } else if (searchType === "em2lm") {
    converted.image = {
      ...converted.image,
      objective: result.objective || "",
      slideCode: result.slideCode || "",
    };
  }

  return converted;
}

export function convertSearchResults(inputJSON, anatomicalArea, searchType) {
  const alignmentSpace = anatomicalArea.toLowerCase() === 'vnc'
    ? 'JRC2018_VNC_Unisex_40x_DS'
    : 'JRC2018_Unisex_20x_HR';
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
    results: inputJSON.results
      ? inputJSON.results.map((result) =>
          convertResult(result, alignmentSpace, anatomicalArea, searchType)
        )
      : [],
  };
  return output;
}
