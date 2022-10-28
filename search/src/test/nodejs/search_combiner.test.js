jest.mock('aws-sdk');
jest.mock('aws-appsync');
jest.mock('../../main/nodejs/awsappsyncutils');
jest.mock('../../main/nodejs/utils');

import * as search_combiner from '../../main/nodejs/search_combiner';

import * as clientDbUtils from '../../main/nodejs/clientDbUtils';
import * as utils from '../../main/nodejs/utils';
import * as searchutils from '../../main/nodejs/searchutils';
import zlib from 'zlib';

import intermediateEMBatchResults from '../resources/test_intermediate_em_batchresult.json';
import finalEMSearchResults from '../resources/test_final_em_searchresult.json';

import intermediateLMBatchResults1 from '../resources/test_intermediate_lm_batchresult-1.json';
import intermediateLMBatchResults2 from '../resources/test_intermediate_lm_batchresult-2.json';
import finalLMSearchResults from '../resources/test_final_lm_searchresult.json';

describe('combine EM SearchResults', () => {
    const searchBucket = 'janelia-neuronbridge-search-devpre';
    const maskFolder = 'private/us-region-1:a-e-0-1-1/1-0-b';
    const maskName = '1537331894-RT-JRC2018_Unisex_20x_HR-CDM_1_mask';
    const combineEMSearches = {
        jobId: "a44f76f0-4f07-11ed-87a8-d3d63f8b8c1a",
        jobParameters: {
            searchId: "b76c6902-7329-4c0e-8112-449d91e652b9",
            dataThreshold: 100,
            pixColorFluctuation: 1,
            xyShift: 2,
            mirrorMask: true,
            minMatchingPixRatio: 2,
            maskThresholds: [
                100
            ],
            maxResultsPerMask: -1,
            searchBucket: searchBucket,
            maskKeys: [
                `${maskFolder}/${maskName}.png`
            ],
            inputAnatomicalRegion: "brain",
            libraries: [
                {
                    store: "fl:open_data:brain",
                    anatomicalArea: "Brain",
                    libraryBucket: "janelia-flylight-color-depth",
                    libraryThumbnailsBucket: "janelia-flylight-color-depth-thumbnails",
                    alignmentSpace: "JRC2018_Unisex_20x_HR",
                    libraryName: "FlyEM_Hemibrain_v1.2.1",
                    publishedNamePrefix: "hemibrain:1.2.1",
                    searchedNeuronsFolder: "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.2.1/searchable_neurons",
                    targetType: "EMImage",
                    lsize: 45018
                }
            ]
        },
        numBatches: 1126,
        searchTimeoutSecs: "400",
        startTime: "2022-10-18T17:09:35.839Z",
        monitorFunctionName: "burst-compute-devpre-monitor",
        combinerFunctionName: "janelia-neuronbridge-cds-devpre-combiner",
        tasksTableName: "burst-compute-devpre-tasks",
        elapsedSecs: 11,
        numRemaining: 0,
        completed: true,
        timedOut: false
    };

    beforeEach(() => {
        jest.resetAllMocks();
    });

    it('combine successful EM search results', async () => {
        jest.spyOn(clientDbUtils, 'queryDb')
            .mockResolvedValueOnce({
                Items: [
                    {
                        resultsMimeType: 'application/gzip',
                        results: zlib.gzipSync(JSON.stringify(intermediateEMBatchResults))
                    }   
                ]
            });
    
        const saveFn = jest.spyOn(utils, 'streamObject')
            .mockResolvedValueOnce(`s3://${searchBucket}`);
        jest.spyOn(utils, 'removeKey');
        jest.spyOn(searchutils, 'getIntermediateSearchResultsPrefix');

        search_combiner.searchCombiner(combineEMSearches)
            .then(result => {
                expect(saveFn).toHaveBeenCalledWith(
                    searchBucket,
                    `${maskFolder}/${maskName}.result`,
                    finalEMSearchResults
                );
            });

    });

});

describe('combine LM SearchResults', () => {
    const searchBucket = 'janelia-neuronbridge-search-devpre';
    const maskFolder = 'private/us-region-1:a-e-0-1-1/1-0-b';
    const maskName = '20x-64G05_AE_01-20150924_19_C1-Brain-JRC2018_Unisex_20x_HR_2_mask';
    const combineLMSearches = {
        jobId: "8eec21a0-56e6-11ed-a46e-7b5d4093201d",
        jobParameters: {
            searchId: "f60d6cde-bbdc-457b-80df-e30f4b948735",
            dataThreshold: 100,
            pixColorFluctuation: 1,
            xyShift: 2,
            mirrorMask: true,
            minMatchingPixRatio: 2,
            maskThresholds: [
                100
            ],
            maxResultsPerMask: -1,
            searchBucket: searchBucket,
            maskKeys: [
                `${maskFolder}/${maskName}.png`
            ],
            inputAnatomicalRegion: "brain",
            libraries: [
                {
                    store: "fl:open_data:brain",
                    anatomicalArea: "Brain",
                    libraryBucket: "janelia-flylight-color-depth",
                    libraryThumbnailsBucket: "janelia-flylight-color-depth-thumbnails",
                    alignmentSpace: "JRC2018_Unisex_20x_HR",
                    libraryName: "FlyLight_Split-GAL4_Drivers",
                    searchedNeuronsFolder: "JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/searchable_neurons",
                    targetType: "LMImage",
                    lsize: 9968
                },
                {
                    store: "fl:open_data:brain",
                    anatomicalArea: "Brain",
                    libraryBucket: "janelia-flylight-color-depth",
                    libraryThumbnailsBucket: "janelia-flylight-color-depth-thumbnails",
                    alignmentSpace: "JRC2018_Unisex_20x_HR",
                    libraryName: "FlyLight_Gen1_MCFO",
                    searchedNeuronsFolder: "JRC2018_Unisex_20x_HR/FlyLight_Gen1_MCFO/searchable_neurons",
                    targetType: "LMImage",
                    lsize: 349358
                },
                {
                    store: "fl:open_data:brain",
                    anatomicalArea: "Brain",
                    libraryBucket: "janelia-flylight-color-depth",
                    libraryThumbnailsBucket: "janelia-flylight-color-depth-thumbnails",
                    alignmentSpace: "JRC2018_Unisex_20x_HR",
                    libraryName: "FlyLight_Annotator_Gen1_MCFO",
                    searchedNeuronsFolder: "JRC2018_Unisex_20x_HR/FlyLight_Annotator_Gen1_MCFO/searchable_neurons",
                    targetType: "LMImage",
                    lsize: 313372
                }
            ]
        },
        numBatches: 9893,
        searchTimeoutSecs: "400",
        startTime: "2022-10-28T17:32:55.866Z",
        monitorFunctionName: "burst-compute-devpre-monitor",
        combinerFunctionName: "janelia-neuronbridge-cds-devpre-combiner",
        tasksTableName: "burst-compute-devpre-tasks",
        elapsedSecs: 235,
        numRemaining: 0,
        completed: true,
        timedOut: false
    };
        
    const OLD_ENV = process.env;

    beforeEach(() => {
        jest.resetAllMocks();
        process.env = {
            ...OLD_ENV,
            STAGE: 'devprod',
            LM_PUBLISHED_STACKS_TABLE: 'lm-published-stacks',
            DEBUG: 'true',
            ...OLD_ENV,
        }
    });

    it('combine successful LM search results', async () => {
        jest.spyOn(clientDbUtils, 'queryDb')
            .mockResolvedValueOnce({
                Items: [
                    {
                        resultsMimeType: 'application/json',
                        results: JSON.stringify(intermediateLMBatchResults1)
                    }   
                ],
                LastEvaluatedKey: 'HasNext',
            })
            .mockResolvedValueOnce({
                Items: [
                    {
                        resultsMimeType: 'application/json',
                        results: JSON.stringify(intermediateLMBatchResults2)
                    }   
                ],
            })
            .mockResolvedValueOnce({
                Items: [
                    {
                        files: {
                            VisuallyLosslessStack: 'https://aws/bucket/Gen1+MCFO/VT007350/VT007350-20180803_63_H2-f-40x-brain-GAL4-JRC2018_Unisex_20x_HR-aligned_stack.h5j',
                        },
                    }   
                ],
            })
            .mockResolvedValueOnce({
                Items: [
                    {
                        files: {
                            VisuallyLosslessStack: 'https://aws/bucket/Split+GAL4/LH2033/LH2033-20160629_31_F6-f-20x-brain-GAL4-JRC2018_Unisex_20x_HR-aligned_stack.h5j',
                        },
                    }   
                ],
            })
            ;
    
        const saveFn = jest.spyOn(utils, 'streamObject')
            .mockResolvedValueOnce(`s3://${searchBucket}`);
        jest.spyOn(utils, 'removeKey');
        jest.spyOn(searchutils, 'getIntermediateSearchResultsPrefix');

        search_combiner.searchCombiner(combineLMSearches)
            .then(result => {
                expect(saveFn).toHaveBeenCalledWith(
                    searchBucket,
                    `${maskFolder}/${maskName}.result`,
                    finalLMSearchResults
                );
            });

    });

});
