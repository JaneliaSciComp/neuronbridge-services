jest.mock('node-fetch', () => jest.fn());
jest.mock('../../main/nodejs/awsappsyncutils');
jest.mock('../../main/nodejs/utils');

import fs from 'fs';
import * as utils from '../../main/nodejs/utils';
import * as awsappsyncutils from '../../main/nodejs/awsappsyncutils';
import * as cds_input from '../../main/nodejs/cds_input';

import { appStarter } from '../../main/nodejs/app_starter';

describe('Color depth search start app entry point', () => {

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

    it('starts color depth search using api gateway and a png mask', async () => {
        const testSearchId = 'testSearchId';
        const input = {
            body: JSON.stringify({
                submittedSearches: [
                    {
                        id: testSearchId,
                        searchMask: 'generatedMIPS/testMIP.png',
                    }
                ],
            }),
        };

        const searchData = {
            id: testSearchId,
            searchId: testSearchId,
            searchMask: 'generatedMIPS/testMIP.png',
            step: 2,
            upload: 'testMIP.png',
            upload_thumbnail: 'upload_thumbnail.png',
            selectedLibraries: ['testLibrary'],
            searchInputFolder: 'src/test/resources/mips',
            identityId: 'testUserId',
            searchDir : 'testSearchDir',
            anatomicalRegion: 'brain',
        };

        prepareMocks(searchData);

        const result = await appStarter(input);

        expect(result.statusCode).toBe(200);
        // Check that displayable mask didn't have to be uploaded to the bucket
        expect(utils.putS3Content).not.toHaveBeenCalled()
        expect(utils.invokeFunction).toHaveBeenCalled();
    });
    
    it('starts color depth search using api gateway and a tif mask', async () => {
        const testSearchId = 'testSearchId';
        const input = {
            body: JSON.stringify({
                submittedSearches: [
                    {
                        id: testSearchId,
                        searchMask: 'generatedMIPS/testMIP.png',
                    }
                ],
            }),
        };

        const searchData = {
            id: testSearchId,
            searchId: testSearchId,
            searchMask: 'generatedMIPS/testMIP.tif',
            step: 2,
            upload: 'testMIP.tif',
            upload_thumbnail: 'upload_thumbnail.png',
            selectedLibraries: ['testLibrary'],
            searchInputFolder: 'src/test/resources/mips',
            identityId: 'testUserId',
            searchDir : 'testSearchDir',
            anatomicalRegion: 'brain',
        };
        const displayableMask = 'testMIP.png';

        prepareMocks(searchData);

        const result = await appStarter(input);

        expect(result.statusCode).toBe(200);
        // Check that displayable mask was uploaded to the bucket
        expect(utils.putS3Content).toHaveBeenCalledWith(
            testBucketName,
            `${searchData.searchInputFolder}/generatedMIPS/${displayableMask}`,
            'image/png',
            expect.any(Buffer)
        );
        expect(utils.invokeFunction).toHaveBeenCalled();
    });

    function prepareMocks(searchData) {
        jest.spyOn(utils, 'getS3ContentAsByteBufferWithRetry')
            .mockResolvedValueOnce(fs.readFileSync(`src/test/resources/mips/${searchData.upload}`));

        jest.spyOn(awsappsyncutils, 'lookupSearchMetadata')
            .mockResolvedValueOnce([]);

        jest.spyOn(awsappsyncutils, 'getSearchMetadata')
            .mockResolvedValueOnce(searchData);

        jest.spyOn(cds_input, 'getSearchInputParams')
            .mockResolvedValueOnce(searchData);

        jest.spyOn(cds_input, 'getSearchedLibraries')
            .mockResolvedValueOnce([
                {
                    store: 'testStore',
                    libraryBucket: 'testLibraryBucket',
                    libraryThumbnailsBucket: 'testLibraryThumbnailsBucket',
                    libraryName: 'testLibrary',
                    targetType: 'EMImage',
                    alignmentSpace: 'JRC2018_Unisex_20x_HR',
                    searchedNeuronsFolder: 'JRC2018_Unisex_20x_HR/testLibrary/searchable_neurons',
                    librarySize: 1000,
                    anatomicalArea: 'Brain',
                }
            ]);

        jest.spyOn(utils, 'invokeFunction')
            .mockResolvedValueOnce({
                Payload: Buffer.from(JSON.stringify({
                    jobId: 'theCDSJobId',
                    numBatches: 50,
                    statusCode: 200,
                    workflowArn: 'stepFunctionCallArn',
                })),
            });
    }
})