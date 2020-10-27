import utils from './utils';

const suffix = "_denormalized.json";

export const denormalize = async (event) => {

    const bucket = event.bucket;
    const prefix = event.folder;
    console.log("Input: ", event);

    const keys = await utils.getAllKeys({ Bucket: bucket, Prefix: prefix });
    console.log(`Found ${keys.length} total keys`);
    const filteredKeys = keys.filter(value => !value.endsWith(suffix));
    console.log(`Filtered to ${filteredKeys.length} keys by removing everything ending with '${suffix}'`);

    const outputUri = await utils.putObject(bucket, prefix+"/keys"+suffix, filteredKeys);
    console.log(`Wrote ${filteredKeys.length} keys to ${outputUri}`);

    const outputUri2 = await utils.putObject(bucket, prefix+"/counts"+suffix, { objectCount : filteredKeys.length });
    console.log(`Wrote counts to ${outputUri2}`);

    return true;
};
