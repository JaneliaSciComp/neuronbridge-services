'use strict';

const {updateSearchMetadata} = require('./awsappsyncutils');

exports.searchUpdate = async (event) => {
    console.log(event);
    let searchData = {
        id: event.searchId,
        step: event.step
    };
    if (event.computedMIPs && event.computedMIPs.length > 0) {
        searchData.computedMIPs = event.computedMIPs;
    }
    return await updateSearchMetadata(searchData);
}
