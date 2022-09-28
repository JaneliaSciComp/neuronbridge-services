function generateMipMatchPath(imageUrl) {
  const parts = imageUrl.split("/");
  const filename = parts.pop();
  parts.push("searchable_neurons/pngs");
  parts.push(filename);
  return parts.join("/");
}

function convertResult(result, anatomicalArea, searchType) {
  // TODO: need to figure out if this is an EM or LM result
  // based on searchType and set the appropriate attributes, eg:
  // neuronType for em results & objective for LM results.

  const converted = {
    image: {
      id: result.id,
      alignmentSpace: "JRC2018_Unisex_20x_HR",
      publishedName: result.publishedName,
      anatomicalArea,
      libraryName: result.libraryName,
      gender: result.gender,
      files: {
        AlignedBodySWC: result.AlignedBodySWC || "",
        ColorDepthMip: result.imageURL,
        ColorDepthMipThumbnail: result.thumbnailURL,
      },
    },
    files: {
      ColorDepthMipInput: result.ColorDepthMipInput || "",
      ColorDepthMipMatch: generateMipMatchPath(result.imageURL),
    },
    mirrored: result.mirrored || false,
    normalizedScore: result.normalizedScore,
    matchingPixels: result.matchingPixels,
  };

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
  const output = {
    inputImage: {
      files: {
        CDSResults: "",
        VisuallyLosslessStack: "",
        ColorDepthMipThumbnail: "",
        ColorDepthMip: inputJSON.maskImageURL
          ? inputJSON.maskImageURL.split("/").slice(-2).join("/")
          : "",
      },
      alignmentSpace: "JRC2018_Unisex_20x_HR",
      id: inputJSON.maskId,
      anatomicalArea,
    },
    results: inputJSON.results
      ? inputJSON.results.map((result) =>
          convertResult(result, anatomicalArea, searchType)
        )
      : [],
  };
  return output;
}
