jest.mock('node-fetch', () => jest.fn());
jest.mock('../../main/nodejs/awsappsyncutils');
jest.mock('../../main/nodejs/utils');

import fs from 'fs';
import * as utils from '../../main/nodejs/utils';
import * as awsappsyncutils from '../../main/nodejs/awsappsyncutils';

import { searchCreateDefaultChannel } from '../../main/nodejs/search_create_default_channel';

describe('Create default channel', () => {

    const OLD_ENV = process.env;
    const testBucketName = 'testSearchBucket';

    beforeEach(() => {
        jest.resetModules(); // Most important - it clears the cache
        process.env = { 
            ...OLD_ENV,
            SEARCH_BUCKET: testBucketName,
        };
    });

    afterAll(() => {
        process.env = OLD_ENV; // Restore old environment
    });
  
    it('creates default channel from png', async () => {
        const testSearchId = 'testSearchId';
        const input = {
            body: JSON.stringify({
                searchId: testSearchId,
            }),
        };

        const searchData = {
            id: testSearchId,
            upload: 'testMIP.png', 
            searchInputFolder: 'src/test/resources/mips',
            identityId: 'testUserId',
            searchDir : 'testSearchDir',
            anatomicalRegion: 'brain',
        };
        const uploadName = searchData.upload.replace(/\.png$/, "_1.png");

        jest.spyOn(utils, 'getS3ContentAsByteBufferWithRetry')
            .mockResolvedValueOnce(fs.readFileSync(`src/test/resources/mips/${searchData.upload}`));

        jest.spyOn(awsappsyncutils, 'getSearchMetadata')
            .mockResolvedValueOnce(searchData);

        const result = await searchCreateDefaultChannel(input);
        expect(result.statusCode).toBe(200);
        expect(result.body).toBe(JSON.stringify({ id: testSearchId }));
        // check that the MIP was copied to the search dir
        expect(utils.copyS3Content).toHaveBeenCalledWith(
            testBucketName,
            `/${testBucketName}/${searchData.searchInputFolder}/${searchData.upload}`,
            `private/${searchData.identityId}/${searchData.searchDir}/generatedMIPS/${uploadName}`
        );
        // check that the thumbnail was generated and uploaded
        expect(utils.putS3Content).toHaveBeenCalledWith(
            testBucketName,
            `private/${searchData.identityId}/${searchData.searchDir}/upload_thumbnail.png`,
            'image/png',
            expect.any(Buffer)
        ); 
    });

    it('creates default channel from tiff', async () => {
        const testSearchId = 'testSearchId';
        const input = {
            body: JSON.stringify({
                searchId: testSearchId,
            }),
        };

        const searchData = {
            id: testSearchId,
            upload: 'testMIP.tif', 
            searchInputFolder: 'src/test/resources/mips',
            identityId: 'testUserId',
            searchDir : 'testSearchDir',
            anatomicalRegion: 'brain',
        };
        const pngUploadName = searchData.upload.replace(/\.tif$/, ".png");
        const uploadName = searchData.upload.replace(/\.tif$/, "_1.png");

        jest.spyOn(utils, 'getS3ContentAsByteBufferWithRetry')
            .mockResolvedValueOnce(fs.readFileSync(`src/test/resources/mips/${searchData.upload}`));

        jest.spyOn(awsappsyncutils, 'getSearchMetadata')
            .mockResolvedValueOnce(searchData);

        const result = await searchCreateDefaultChannel(input);
        expect(result.statusCode).toBe(200);
        expect(result.body).toBe(JSON.stringify({ id: testSearchId }));
        // check that the converted image to PNG was uploaded
        expect(utils.putS3Content).toHaveBeenCalledWith(
            testBucketName,
            `${searchData.searchInputFolder}/${pngUploadName}`,
            'image/png',
            expect.any(Buffer)
        ); 
        // check that the MIP was copied to the search dir
        expect(utils.copyS3Content).toHaveBeenCalledWith(
            testBucketName,
            `/${testBucketName}/${searchData.searchInputFolder}/${pngUploadName}`,
            `private/${searchData.identityId}/${searchData.searchDir}/generatedMIPS/${uploadName}`
        );
        // check that the thumbnail was generated and uploaded
        expect(utils.putS3Content).toHaveBeenCalledWith(
            testBucketName,
            `private/${searchData.identityId}/${searchData.searchDir}/upload_thumbnail.png`,
            'image/png',
            expect.any(Buffer)
        ); 
    });

})