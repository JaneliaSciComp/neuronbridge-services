import { isType } from 'graphql';
import * as batch_search from '../../main/nodejs/batch_search';
import * as load_mip from '../../main/nodejs/load_mip';
import * as utils from '../../main/nodejs/utils';


describe('batchSearch', () => {
    const jobParameters = {
        searchId: 'aSearch',
        dataThreshold: 100,
        pixColorFluctuation: 2,
        xyShift: 2,
        mirrorMask: true,
        minMatchingPixRatio: 2,
        searchBucket: "janelia-neuronbridge-search-dev",
        maskKeys: [
            "private/us-east-1:429a5c9a-c76e-4309-8146-15d991d133f6/1bb075f0-4b11-11ed-b0bb-41bf36e8d453/generatedMIPS/1002360103-RT-JRC2018_Unisex_20x_HR-CDM.png"
        ],
        maskThresholds: [
            100
        ],
        maxResultsPerMask: -1,
        inputAnatomicalRegion: 'brain',
        libraries: [ 
            {
                store: "fl:brain:1.0",
                anatomicalArea: 'Brain',
                targetType: 'EMImage',
                libraryBucket: 'janelia-flylight-color-depth',
                libraryThumbnailsBucket: 'janelia-flylight-color-depth-thumbnails',
                alignmentSpace: 'JRC2018_Unisex_20x_HR',
                libraryName: "FlyEM_Hemibrain_v1.0",
                searchedNeuronsFolder: 'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/searchable_neurons',
                lsize: 5
            }, {
                store: "fl:brain:2.0",
                anatomicalArea: 'Brain',
                targetType: 'EMImage',
                libraryBucket: 'janelia-flylight-color-depth-devpre',
                libraryThumbnailsBucket: 'janelia-flylight-color-depth-devpre-thumbnails',
                alignmentSpace: 'JRC2018_Unisex_20x_HR',
                libraryName: "FlyEM_Hemibrain_v2.0",
                searchedNeuronsFolder: 'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/searchable_neurons',
                lsize: 5
            }, {
                store: "fl:brain:3.0",
                anatomicalArea: 'Brain',
                targetType: 'EMImage',
                libraryBucket: 'janelia-flylight-color-depth-devpre',
                libraryThumbnailsBucket: 'janelia-flylight-color-depth-devpre-thumbnails',
                alignmentSpace: 'JRC2018_Unisex_20x_HR',
                libraryName: "FlyEM_Hemibrain_v3.0",
                searchedNeuronsFolder: 'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/searchable_neurons',
                lsize: 5

            }
        ],
    };

    beforeEach(() => {
        jest.resetAllMocks();
    });

    it('execute a batch that spans 3 libraries', async () => {
        const batchEvenParams = {
            jobId: 'aJob',
            batchId: 2,
            jobParameters: jobParameters,
            startIndex: 3,
            endIndex: 12,
            tasksTableName: "burst-compute-cgdev-tasks",
            level: 2,
            numLevels: 2,
            batchSize: 72,
            numBatches: 9924,
            branchingFactor: 100,
        };

        const testData = createTestData();
        
        jest.spyOn(load_mip, 'loadMIPRange')
            .mockResolvedValueOnce({data: testData.mask, width: testData.width, height: testData.height})
            .mockResolvedValue({data: testData.target, width: testData.width, height: testData.height})
            ;
    
        jest.spyOn(utils, 'getObjectWithRetry')
            .mockResolvedValueOnce([
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.1-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.2-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.3-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.4-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.5-CDM.png',
            ])
            .mockResolvedValueOnce([
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.1-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.2-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.3-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.4-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.5-CDM.png',
            ])
            .mockResolvedValueOnce([
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.1-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.2-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.3-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.4-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.5-CDM.png',
            ])
            ;
        
        const putDbItemSpy = jest.spyOn(utils, 'putDbItemWithRetry').mockResolvedValue();

        batch_search.batchSearch(batchEvenParams)
            .then(result => {
                expect(putDbItemSpy).toHaveBeenCalledTimes(1);
            });
    });

    it('execute a batch with one library', async () => {
        const batchEvenParams = {
            jobId: 'aJob',
            batchId: 2,
            jobParameters: jobParameters,
            startIndex: 6,
            endIndex: 7,
            tasksTableName: "burst-compute-cgdev-tasks",
            level: 2,
            numLevels: 2,
            batchSize: 72,
            numBatches: 9924,
            branchingFactor: 100,
        };

        const testData = createTestData();
        
        jest.spyOn(load_mip, 'loadMIPRange')
            .mockResolvedValueOnce({data: testData.mask, width: testData.width, height: testData.height})
            .mockResolvedValue({data: testData.target, width: testData.width, height: testData.height})
            ;
    
        jest.spyOn(utils, 'getObjectWithRetry')
            .mockResolvedValueOnce([
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.1-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.2-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.3-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.4-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.5-CDM.png',
            ])
            .mockResolvedValueOnce([
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.1-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.2-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.3-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.4-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v2.0/1002360103-RT-JRC2018_Unisex_20x_HR-2.5-CDM.png',
            ])
            .mockResolvedValueOnce([
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.1-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.2-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.3-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.4-CDM.png',
                'JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v3.0/1002360103-RT-JRC2018_Unisex_20x_HR-3.5-CDM.png',
            ])
            ;
        
        const putDbItemSpy = jest.spyOn(utils, 'putDbItemWithRetry').mockResolvedValue();

        batch_search.batchSearch(batchEvenParams)
            .then(result => {
                const expectedMatcResults = [
                    {
                        maskId:"1002360103-RT-JRC2018_Unisex_20x_HR-CDM",
                        maskLibraryName:null,
                        maskPublishedName:null,
                        maskImageURL: "https://s3.amazonaws.com/janelia-neuronbridge-search-dev/private/us-east-1:429a5c9a-c76e-4309-8146-15d991d133f6/1bb075f0-4b11-11ed-b0bb-41bf36e8d453/generatedMIPS/1002360103-RT-JRC2018_Unisex_20x_HR-CDM.png",
                        results: [
                            {
                                maskImageName: "private/us-east-1:429a5c9a-c76e-4309-8146-15d991d133f6/1bb075f0-4b11-11ed-b0bb-41bf36e8d453/generatedMIPS/1002360103-RT-JRC2018_Unisex_20x_HR-CDM.png",
                                imageURL: "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.2-CDM.png",
                                thumbnailURL: "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.2-CDM.jpg",
                                id: "1002360103-RT-JRC2018_Unisex_20x_HR-1.2-CDM",
                                libraryStore: "fl:brain:2.0",
                                targetType: "EMImage",
                                libraryName: "FlyEM_Hemibrain_v2.0",
                                publishedName: "1002360103",
                                imageName: "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1002360103-RT-JRC2018_Unisex_20x_HR-1.2-CDM.png",
                                gender: "f",
                                anatomicalArea: "Brain",
                                alignmentSpace: "JRC2018_Unisex_20x_HR",
                                matchingPixels: 20000,
                                matchingRatio: 1,
                                mirrored: false,
                                gradientAreaGap: -1,
                                normalizedScore: 20000
                            }
                        ]
                    }
                ];
                expect(putDbItemSpy).toHaveBeenNthCalledWith(
                    1,
                    'burst-compute-cgdev-tasks',
                    expect.objectContaining({
                        jobId: {S: 'aJob'},
                        batchId: {N: '2'},
                        results: {S: JSON.stringify(expectedMatcResults)}
    
                    })
                );
            });
    });

    const createTestData = () => {
        const width = 1210;
        const height = 566;
    
        let testdata = new Uint8Array(width * height * 3);
        for (let y = 300; y < 400; y++)
        {
            for (let x = 100; x < 200; x++)
            {
                testdata[(y * width + x) * 3 + 0] = 254;
                testdata[(y * width + x) * 3 + 1] = 255;
                testdata[(y * width + x) * 3 + 2] = 0;
            }
            for (let x = 200; x < 300; x++)
            {
                testdata[(y * width + x) * 3 + 0] = 54;
                testdata[(y * width + x) * 3 + 1] = 255;
                testdata[(y * width + x) * 3 + 2] = 201;
            }
        }
    
        let testdata2 = new Uint8Array(width * height * 3);
        for (let y = 300; y < 400; y++)
        {
            for (let x = 102; x < 202; x++)
            {
                testdata2[(y * width + x) * 3 + 0] = 254;
                testdata2[(y * width + x) * 3 + 1] = 255;
                testdata2[(y * width + x) * 3 + 2] = 0;
            }
            for (let x = 202; x < 302; x++)
            {
                testdata2[(y * width + x) * 3 + 0] = 54;
                testdata2[(y * width + x) * 3 + 1] = 255;
                testdata2[(y * width + x) * 3 + 2] = 201;
            }
        }

        return {
            width,
            height,
            mask: testdata,
            target: testdata2,
        };
    }
});
