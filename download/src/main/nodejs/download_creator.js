// import archiver from "archiver";
import {
  getSearchMetadata,
} from "./awsappsyncutils";

async function getSearchRecord(searchId) {
  const searchMetadata = await getSearchMetadata(searchId);
  return searchMetadata;
}

export const downloadCreator = async event => {
  // Accept list of selected ids and the resultSet id/path
  const { ids = [], resultId = "" } = event.body
    ? JSON.parse(event.body) : {};

  // Create an archive that streams directly to the download bucket.
  /* const archive = archiver("tar", {
    gzip: true,
    gzipOptions: {
      level: 1
    }
  });
  console.log(archive); */
  // archive.pipe(res);

  // grab the list of results using the resultSet id/path
  const searchRecord = getSearchRecord(resultId);

  // Loop over the ids and push them into the archive.
  const added = [];
  ids.forEach(id => {
    // Use the information in the resultSet object to find the image path
    // Pass the image from the source bucket into the download bucket via
    // the archiver.
    added.push(id);
  });

  // Once all image transfers are complete, close the archive
  // archive.finalize();

  // Create a link to the newly created archive file and return it
  // as the response.
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({ message: "huzzah", ids: added, resultId, searchRecord })
  };

  return returnObj;
};
