'use strict';

const {updateSearchMetadata} = require('./awsappsyncutils');

exports.searchUpdate = async (event) => {
    console.log(event);
    return await updateSearchMetadata({
        id: event.searchId,
        step: event.step
    });
}
