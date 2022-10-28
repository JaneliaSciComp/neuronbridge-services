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

describe('combineSearchResults', () => {
    const searchBucket = 'janelia-neuronbridge-search-devpre';
    const maskFolder = 'private/us-region-1:a-e-0-1-1/1-0-b';
    const maskName = '1537331894-RT-JRC2018_Unisex_20x_HR-CDM_1_mask';
    const combineEvent = {
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

        search_combiner.searchCombiner(combineEvent)
            .then(result => {
                expect(saveFn).toHaveBeenCalledWith(
                    searchBucket,
                    `${maskFolder}/${maskName}.result`,
                    finalEMSearchResults
                );
            });

    });

});
