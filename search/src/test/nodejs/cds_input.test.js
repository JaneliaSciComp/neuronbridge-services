jest.mock('aws-appsync');
jest.mock('../../main/nodejs/awsappsyncutils');

import * as utils from '../../main/nodejs/utils';

import { getSearchedLibraries } from '../../main/nodejs/cds_input';

import testConfig from '../resources/test_config.json';

describe('prepare custom cds input', () => {

    const OLD_ENV = process.env;

    beforeEach(() => {
        jest.resetModules(); // Most important - it clears the cache
        process.env = { 
            ...OLD_ENV,
            STAGE: 'devprod',
            DEBUG: 'true',
            ...OLD_ENV,
        };
    });
  
    afterAll(() => {
      process.env = OLD_ENV; // Restore old environment
    });
  
    it('get searchable input libraries for EM search', async () => {
        jest.spyOn(utils, 'getS3ContentWithRetry')
            .mockResolvedValueOnce(Buffer.from('version value', 'utf8'));
        jest.spyOn(utils, 'getObjectWithRetry')
            .mockResolvedValueOnce(testConfig)
            .mockResolvedValue({ objectCount: 10 });
        const inputSearchedData = {
            searchId: '54e4-0-d-aba9-54afb4',
            searchType: 'lm2em',
            anatomicalRegion: 'brain',
        };
        const testBucketName = 'testDataBucket';
        const searchedData = await getSearchedLibraries(inputSearchedData, testBucketName);
        console.log(searchedData);
        expect(searchedData.searchType).toBe('lm2em');
        expect(searchedData.anatomicalRegion).toBe('brain');
        expect(searchedData.searchedLibraries.length).toBe(2);
        expect(searchedData.totalSearches).toBeGreaterThan(0);
        searchedData.searchedLibraries.forEach(lc => {
            expect(lc.libraryBucket).toContain('janelia-flylight-color-depth');
            expect(lc.libraryThumbnailsBucket).toContain('janelia-flylight-color-depth');
            expect(lc.libraryThumbnailsBucket).toContain('thumbnails');
            expect(lc.libraryName).toBeDefined();
            expect(lc.publishedNamePrefix).toBeDefined();
            expect(lc.anatomicalArea).toBe('Brain');
            expect(lc.targetType).toBe('EMImage');
            expect(lc.alignmentSpace).toBe('JRC2018_Unisex_20x_HR');
            expect(lc.hasOwnProperty('libraryName')).toBe(true);
            expect(lc.hasOwnProperty('searchedNeuronsFolder')).toBe(true);
            expect(lc.searchedNeuronsFolder).toBe(`${lc.alignmentSpace}/${lc.libraryName}/searchable_neurons`);
        })
    });

    it('get searchable input libraries for LM search', async () => {  
        jest.spyOn(utils, 'getS3ContentWithRetry')
            .mockResolvedValueOnce(Buffer.from('version value', 'utf8'));
        jest.spyOn(utils, 'getObjectWithRetry')
            .mockResolvedValueOnce(testConfig)
            .mockResolvedValue({ objectCount: 10 })
            ;
        const inputSearchedData = {
            searchId: '54e4-0-d-aba9-54afb4',
            searchType: 'em2lm',
            anatomicalRegion: 'vnc',
        };
        const testBucketName = 'testDataBucket';
        const searchedData = await getSearchedLibraries(inputSearchedData, testBucketName);
        console.log(searchedData);
        expect(searchedData.searchType).toBe('em2lm');
        expect(searchedData.anatomicalRegion).toBe('vnc');
        expect(searchedData.searchedLibraries.length).toBe(4);
        expect(searchedData.totalSearches).toBeGreaterThan(0);
        searchedData.searchedLibraries.forEach(lc => {
            expect(lc.libraryBucket).toContain('janelia-flylight-color-depth');
            expect(lc.libraryThumbnailsBucket).toContain('janelia-flylight-color-depth');
            expect(lc.libraryThumbnailsBucket).toContain('thumbnails');
            expect(lc.libraryName).toBeDefined();
            expect(lc.publishedNamePrefix).toBeUndefined();
            expect(lc.anatomicalArea).toBe('VNC');
            expect(lc.targetType).toBe('LMImage');
            expect(lc.alignmentSpace).toBe('JRC2018_VNC_Unisex_40x_DS');
            expect(lc.hasOwnProperty('libraryName')).toBe(true);
            expect(lc.hasOwnProperty('searchedNeuronsFolder')).toBe(true);
            expect(lc.searchedNeuronsFolder).toBe(`${lc.alignmentSpace}/${lc.libraryName}/searchable_neurons`);
        })
    });

    it('get searchable input for invalid searchType', async () => {
        jest.spyOn(utils, 'getS3ContentWithRetry')
            .mockResolvedValueOnce(Buffer.from('version value', 'utf8'));
        jest.spyOn(utils, 'getObjectWithRetry')
            .mockResolvedValueOnce(testConfig)
            .mockResolvedValue({ objectCount: 10 })
            ;
        const inputSearchedData = {
            searchId: '54e4-0-d-aba9-54afb4',
            searchType: 'invalid',
            anatomicalRegion: 'vnc',
        };
        const testBucketName = 'testDataBucket';
        const searchedData = await getSearchedLibraries(inputSearchedData, testBucketName);
        expect(searchedData.totalSearches).toBe(0);
        expect(searchedData.searchedLibraries.length).toBe(0);
    });

});
