import {updateSearchMetadata} from './awsappsyncutils';

export const searchUpdate = async (event) => {
    console.log(event);
    let searchData = {
        id: event.searchId,
        step: event.step
    };
    if (event.computedMIPs && event.computedMIPs.length > 0) {
        searchData.computedMIPs = event.computedMIPs;
    }
    if (event.uploadThumbnail) {
        searchData.uploadThumbnail = event.uploadThumbnail;
    }
    if (event.displayableMask) {
        searchData.displayableMask = event.displayableMask;
    }
    if (event.errorMessage) {
        searchData.errorMessage = event.errorMessage;
    }
    if (event.alignmentErrorMessage) {
        searchData.alignmentErrorMessage = event.alignmentErrorMessage;
    }
    if (event.alignFinished) {
        searchData.alignFinished = event.alignFinished;
    }
    if (event.alignmentMovie) {
        searchData.alignmentMovie = event.alignmentMovie;
    }
    if (event.alignmentScore) {
        searchData.alignmentScore = event.alignmentScore;
    }
    return await updateSearchMetadata(searchData);
};
